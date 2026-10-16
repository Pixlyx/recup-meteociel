#!/usr/bin/env python3
"""
Récupère les prévisions Meteociel d'une ville et écrit un CSV par modèle.

- Les requêtes sont espacées (DELAY_S + un peu d'aléatoire) et réessayées avec
  une attente croissante en cas d'erreur 429 / 5xx.
- Un modèle n'est re-téléchargé que s'il est "dû" (MIN_REFRESH_H écoulées depuis
  son dernier changement) et son CSV n'est réécrit que si un nouveau run est
  détecté (ligne « Réactualisé à … » + première échéance du tableau).
- L'état est gardé dans data/state.json pour survivre entre deux exécutions.

Usage :
    python meteociel_scraper.py                 # modèles dus uniquement
    python meteociel_scraper.py --force         # tout re-télécharger
    python meteociel_scraper.py --models gfs wrf_1h
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
BASE = "https://www.meteociel.fr"
VILLE_ID = "64"            # identifiant dans l'URL Meteociel
VILLE_SLUG = "certines"    # nom de la ville dans l'URL

# nom du CSV -> (dossier d'URL Meteociel, heures minimum entre deux contrôles
#                après un changement). Ces durées sont des estimations de la
#                fréquence des runs : à ajuster si besoin.
MODELS: dict[str, tuple[str, float]] = {
    "gfs":       ("previsions",            5),
    "wrf":       ("previsions-wrf",        5),
    "wrf_1h":    ("previsions-wrf-1h",     5),
    "arome":     ("previsions-arome",      2),
    "arome_1h":  ("previsions-arome-1h",   2),
    "arpege_1h": ("previsions-arpege-1h",  5),
    "iconeu":    ("previsions-iconeu",     5),
    "icond2":    ("previsions-icond2",     2),
}

DATA_DIR = Path(os.environ.get("METEO_DATA_DIR", "data"))
STATE_FILE = DATA_DIR / "state.json"

DELAY_S = 20.0             # pause entre deux requêtes
JITTER_S = 5.0             # + 0 à 5 s aléatoires
TIMEOUT_S = 30
BACKOFF_S = [60, 120, 240]  # attentes avant les réessais (429, 5xx, réseau)
RETRYABLE = {429, 500, 502, 503, 504}

TZ = ZoneInfo("Europe/Paris")
USER_AGENT = "meteo-fetch/1.0 (collecte personnelle, faible frequence)"

JOURS = {"Lun": 0, "Mar": 1, "Mer": 2, "Jeu": 3, "Ven": 4, "Sam": 5, "Dim": 6}

CSV_FIELDS = [
    "modele", "run_modele", "run_z", "maj_affichee", "recupere_le_utc",
    "date", "jour", "heure",
    "temp_c", "temp_ressentie_c",
    "vent_dir", "vent_dir_deg", "vent_moy_kmh", "vent_raf_kmh",
    "pluie_mm", "humidite_pct", "pression_hpa", "temps",
]

# « Réactualisé à 06:01 (run GFS de 0Z) » (tolérant aux soucis d'accents)
UPDATE_RE = re.compile(
    r"R\S{1,3}actualis\S{0,3}\s+\S{1,3}\s+(\d{1,2}:\d{2})\s*\(run\s+(.+?)\s+de\s+(\d{1,2})Z\)",
    re.I,
)

# Une ligne du tableau, une fois les cellules mises bout à bout :
# « Ven 16 05:00 12 °C 12 Sud-Est : 143 ° 5 5 -- 90 % 1024 hPa Peu nuageux »
ROW_RE = re.compile(
    r"""
    (?:(?P<jour>Lun|Mar|Mer|Jeu|Ven|Sam|Dim)\s*(?P<num>\d{1,2}))?
    [^\d]*?
    (?P<heure>\d{1,2}:\d{2})\s+
    (?P<temp>-?\d+)\s*°\s*C\s+
    (?P<ressenti>-?\d+)\s+
    (?P<dir>[A-Za-zÀ-ÿ\- ]+?)\s*:\s*(?P<deg>\d+)\s*°\s+
    (?P<moy>\d+)\s+(?P<raf>\d+)\s+
    (?P<pluie>--|[<>]?\s*\d+(?:[.,]\d+)?\s*mm)\s+
    (?P<hum>\d+)\s*%\s+
    (?P<pres>\d+)\s*hPa\s*
    (?P<temps>.*)$
    """,
    re.X,
)


def log(msg: str) -> None:
    print(f"[{datetime.now(TZ):%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Téléchargement
# --------------------------------------------------------------------------- #
def model_url(name: str) -> str:
    return f"{BASE}/{MODELS[name][0]}/{VILLE_ID}/{VILLE_SLUG}.htm"


def retry_after_seconds(value: str | None) -> float | None:
    if value and value.strip().isdigit():
        return min(float(value), 600.0)
    return None


def fetch(session: requests.Session, url: str) -> str:
    err = "erreur inconnue"
    for attempt in range(len(BACKOFF_S) + 1):
        wait: float | None = None
        try:
            r = session.get(url, timeout=TIMEOUT_S)
            if r.status_code == 200:
                return r.text
            err = f"HTTP {r.status_code}"
            if r.status_code not in RETRYABLE:
                raise RuntimeError(err)
            wait = retry_after_seconds(r.headers.get("Retry-After"))
        except requests.RequestException as exc:
            err = str(exc)
        if attempt == len(BACKOFF_S):
            break
        wait = wait or BACKOFF_S[attempt]
        log(f"  {err} -> nouvel essai dans {wait:.0f} s")
        time.sleep(wait)
    raise RuntimeError(f"échec après {len(BACKOFF_S) + 1} essais ({err})")


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #
def cell_text(cell) -> str:
    text = cell.get_text(" ", strip=True)
    if not text:  # pictogramme météo : le libellé est dans l'image
        img = cell.find("img")
        if img is not None:
            text = (img.get("alt") or img.get("title") or "").strip()
    return text


def parse_page(html: str, today: date) -> tuple[dict, list[dict]]:
    soup = BeautifulSoup(html, "html.parser")

    m = UPDATE_RE.search(soup.get_text(" ", strip=True))
    meta = {
        "maj_affichee": m.group(1) if m else "",
        "run_modele": m.group(2).strip() if m else "",
        "run_z": int(m.group(3)) if m else "",
    }
    if not m:
        log("  attention : ligne « Réactualisé à … » introuvable")

    raw: list[dict] = []
    for tr in soup.find_all("tr"):
        if tr.find("tr") is not None:  # on ne garde que les lignes les plus internes
            continue
        text = " ".join(cell_text(c) for c in tr.find_all(["td", "th"]))
        text = re.sub(r"\s+", " ", text).strip()
        row = ROW_RE.match(text)
        if row:
            raw.append(row.groupdict())
    if not raw:
        raise ValueError("tableau de prévisions introuvable (page modifiée ?)")

    # Dates : le tableau n'indique que « Ven 16 » -> on reconstitue la date complète.
    rows: list[dict] = []
    current: date | None = None
    for r in raw:
        if r["jour"]:
            num, wd = int(r["num"]), JOURS[r["jour"]]
            d = (current + timedelta(days=1)) if current else today - timedelta(days=1)
            for _ in range(10):
                if d.day == num and d.weekday() == wd:
                    break
                d += timedelta(days=1)
            else:
                raise ValueError(f"date introuvable pour « {r['jour']} {r['num']} »")
            current = d
        if current is None:
            raise ValueError("ligne sans jour avant la première date")
        pluie = r["pluie"]
        rows.append({
            "date": current.isoformat(),
            "jour": [k for k, v in JOURS.items() if v == current.weekday()][0],
            "heure": r["heure"].zfill(5),
            "temp_c": int(r["temp"]),
            "temp_ressentie_c": int(r["ressenti"]),
            "vent_dir": r["dir"].strip(),
            "vent_dir_deg": int(r["deg"]),
            "vent_moy_kmh": int(r["moy"]),
            "vent_raf_kmh": int(r["raf"]),
            "pluie_mm": 0.0 if pluie == "--"
            else float(re.sub(r"[<>\smm]", "", pluie).replace(",", ".")),
            "humidite_pct": int(r["hum"]),
            "pression_hpa": int(r["pres"]),
            "temps": r["temps"].strip(),
        })
    return meta, rows


def run_key(meta: dict, rows: list[dict]) -> str:
    """Empreinte d'un run : change dès que la page est réactualisée."""
    first = rows[0]
    return "|".join(str(x) for x in (
        meta["maj_affichee"], meta["run_modele"], meta["run_z"],
        first["date"], first["heure"], len(rows),
    ))


# --------------------------------------------------------------------------- #
# Sorties et état
# --------------------------------------------------------------------------- #
def write_csv(name: str, meta: dict, rows: list[dict], fetched_utc: str) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{name}.csv"
    tmp = path.with_suffix(".csv.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({"modele": name, "recupere_le_utc": fetched_utc, **meta, **r})
    tmp.replace(path)
    return path


def load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def is_due(entry: dict | None, min_hours: float, now: datetime) -> bool:
    if not entry or "last_change_utc" not in entry:
        return True
    last = datetime.fromisoformat(entry["last_change_utc"])
    return now - last >= timedelta(hours=min_hours)


# --------------------------------------------------------------------------- #
# Programme principal
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", nargs="+", choices=list(MODELS), default=list(MODELS))
    ap.add_argument("--force", action="store_true", help="ignore l'état : télécharge et réécrit tout")
    ap.add_argument("--delay", type=float, default=DELAY_S, help="secondes entre deux requêtes")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    today = datetime.now(TZ).date()
    state = load_state()

    todo = [m for m in args.models
            if args.force or is_due(state.get(m), MODELS[m][1], now)]
    skipped = [m for m in args.models if m not in todo]
    if skipped:
        log(f"ignorés (mis à jour récemment) : {', '.join(skipped)}")
    if not todo:
        log("rien à faire")
        return 0

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "fr-FR,fr;q=0.9"})

    failures = 0
    for i, name in enumerate(todo):
        if i:
            pause = args.delay + random.uniform(0, JITTER_S)
            log(f"pause de {pause:.0f} s")
            time.sleep(pause)
        log(f"{name} : {model_url(name)}")
        try:
            html = fetch(session, model_url(name))
            meta, rows = parse_page(html, today)
        except Exception as exc:  # noqa: BLE001 - on continue avec les autres modèles
            failures += 1
            log(f"  ERREUR : {exc}")
            continue

        key = run_key(meta, rows)
        entry = state.setdefault(name, {})
        entry["last_check_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if key == entry.get("key") and not args.force:
            log(f"  pas de nouveau run ({meta['maj_affichee']}, {meta['run_modele']} {meta['run_z']}Z)")
            continue

        fetched = datetime.now(timezone.utc).isoformat(timespec="seconds")
        path = write_csv(name, meta, rows, fetched)
        entry.update(key=key, last_change_utc=fetched)
        log(f"  {len(rows)} lignes -> {path} (run {meta['run_modele']} {meta['run_z']}Z, maj {meta['maj_affichee']})")
        save_state(state)  # on sauvegarde au fil de l'eau

    save_state(state)
    return 1 if failures == len(todo) else 0


if __name__ == "__main__":
    sys.exit(main())
