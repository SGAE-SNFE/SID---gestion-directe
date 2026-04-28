import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import colorlog
import pandas as pd
import requests
from tqdm import tqdm

from fts_enrichissement import enrichir_procedure1, exporter_xlsx_colore


# Le serveur CE a changé d'URL à un moment — celle-ci fonctionne en mai 2026.
# On garde l'ancienne en secours au cas où ils basculent encore.
FTS_BASE_URL = (
    "https://ec.europa.eu/budget/financial-transparency-system/download/"
    "{annee}_FTS_dataset_en.xlsx"
)
FTS_ALT_URL = (
    "https://commission.europa.eu/document/"
    "{annee}_FTS_dataset_en.xlsx"
)

# Sans ces headers, le serveur renvoie une page HTML au lieu du fichier.
# User-Agent + Referer suffisent à passer la protection anti-scraping.
FTS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://ec.europa.eu/budget/financial-transparency-system/help.html",
    "Accept": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
        "application/octet-stream,*/*"
    ),
}

ANNEE_MIN_DEFAULT = 2014
ANNEE_MAX_DEFAULT = 2024

DIR_RAW          = Path("data/raw")
DIR_PROCESSED    = Path("data/processed")
DIR_LOGS         = Path("logs")
DIR_REFERENTIELS = Path("referentiels")


# ---- logs -------------------------------------------------------------------

def setup_logging(log_file: Path) -> logging.Logger:
    DIR_LOGS.mkdir(parents=True, exist_ok=True)

    handler_console = colorlog.StreamHandler()
    handler_console.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s [%(levelname)-8s]%(reset)s %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            "DEBUG":    "cyan",
            "INFO":     "green",
            "WARNING":  "yellow",
            "ERROR":    "red",
            "CRITICAL": "bold_red",
        }
    ))

    handler_file = logging.FileHandler(log_file, encoding="utf-8")
    handler_file.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))

    logger = logging.getLogger("fts_pipeline")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler_console)
    logger.addHandler(handler_file)
    return logger


# ---- étape 1 : téléchargement -----------------------------------------------

def _est_xlsx_valide(chemin: Path) -> bool:
    # Un vrai xlsx est un zip, donc commence toujours par "PK".
    # Si on reçoit autre chose, c'est que le serveur a renvoyé une page d'erreur HTML.
    try:
        with open(chemin, "rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False


def telecharger_fichier_fts(annee: int, repertoire: Path, logger: logging.Logger, forcer: bool = False) -> Path | None:
    repertoire.mkdir(parents=True, exist_ok=True)
    chemin_local = repertoire / f"{annee}_FTS_dataset_en.xlsx"

    if chemin_local.exists() and not forcer:
        if _est_xlsx_valide(chemin_local):
            taille_mo = chemin_local.stat().st_size / 1024 ** 2
            logger.info(f"  [{annee}] déjà présent ({taille_mo:.1f} Mo) — on passe")
            return chemin_local
        else:
            # Fichier corrompu ou téléchargement interrompu — on repart de zéro
            logger.warning(f"  [{annee}] fichier invalide sur le disque — suppression et retry")
            try:
                chemin_local.unlink()
            except PermissionError:
                logger.error(f"  [{annee}] impossible de supprimer {chemin_local.name} — ferme le fichier et relance")
                return None

    for url in [FTS_BASE_URL.format(annee=annee), FTS_ALT_URL.format(annee=annee)]:
        logger.info(f"  [{annee}] essai : {url}")
        try:
            response = requests.get(url, timeout=180, stream=True, headers=FTS_HEADERS)

            if response.status_code != 200:
                logger.warning(f"  [{annee}] HTTP {response.status_code} — on essaie l'url suivante")
                time.sleep(2)
                continue

            content_type = response.headers.get("content-type", "")
            if "html" in content_type.lower():
                logger.warning(f"  [{annee}] réponse HTML reçue ({content_type}) — URL probablement obsolète")
                time.sleep(2)
                continue

            taille_attendue = int(response.headers.get("content-length", 0))
            with open(chemin_local, "wb") as f:
                with tqdm(total=taille_attendue, unit="B", unit_scale=True, desc=f"    {annee}", leave=False) as barre:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        barre.update(len(chunk))

            if not _est_xlsx_valide(chemin_local):
                logger.warning(f"  [{annee}] fichier reçu invalide — supprimé")
                chemin_local.unlink()
                time.sleep(2)
                continue

            taille_reelle = chemin_local.stat().st_size
            logger.info(f"  [{annee}] ok — {taille_reelle / 1024:.0f} Ko")
            return chemin_local

        except requests.Timeout:
            logger.warning(f"  [{annee}] timeout sur {url}")
        except requests.RequestException as e:
            logger.warning(f"  [{annee}] erreur réseau : {e}")

        time.sleep(2)

    logger.error(f"  [{annee}] échec — aucune URL n'a fonctionné")
    logger.error(f"  [{annee}] vérifier manuellement : https://ec.europa.eu/budget/financial-transparency-system/")
    return None


def telecharger_tous_les_fichiers(annees: list, repertoire: Path, logger: logging.Logger, forcer: bool = False) -> dict:
    logger.info("=" * 60)
    logger.info(f"ÉTAPE 1 — Téléchargement ({len(annees)} fichiers : {annees[0]}-{annees[-1]})")
    logger.info("=" * 60)

    resultats = {}
    for annee in annees:
        chemin = telecharger_fichier_fts(annee, repertoire, logger, forcer)
        if chemin:
            resultats[annee] = chemin

    logger.info(f"  {len(resultats)}/{len(annees)} fichiers disponibles")
    return resultats


# ---- étape 2 : consolidation ------------------------------------------------

def charger_fichier_fts(chemin: Path, annee: int, logger: logging.Logger) -> pd.DataFrame | None:
    # On lit tout en string pour éviter que pandas interprète les montants ou les codes postaux
    logger.info(f"  [{annee}] chargement de {chemin.name}")
    try:
        df = pd.read_excel(chemin, sheet_name=0, dtype=str, engine="openpyxl")
        logger.info(f"  [{annee}] ok — {len(df):,} lignes, {len(df.columns)} colonnes")
        return df
    except Exception as e:
        logger.error(f"  [{annee}] erreur de lecture : {type(e).__name__} — {e}")
        return None


def consolider(fichiers: dict, logger: logging.Logger) -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info("ÉTAPE 2 — Consolidation")
    logger.info("=" * 60)

    frames = []
    for annee in sorted(fichiers.keys()):
        df = charger_fichier_fts(fichiers[annee], annee, logger)
        if df is not None:
            frames.append(df)

    if not frames:
        logger.critical("aucun fichier chargé — arrêt")
        sys.exit(1)

    df_total = pd.concat(frames, ignore_index=True)
    logger.info(f"  consolidé : {len(df_total):,} lignes | années {sorted(fichiers.keys())}")
    return df_total


# ---- arguments --------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Téléchargement + consolidation + enrichissement des fichiers FTS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
exemples :
  python fts_pipeline.py                          -> toutes les années
  python fts_pipeline.py --annees 2022 2023 2024  -> années précises
  python fts_pipeline.py --annee-min 2021         -> à partir de 2021
  python fts_pipeline.py --forcer-telechargement  -> re-télécharger même si déjà présent
        """
    )
    parser.add_argument("--annees", nargs="+", type=int, help="liste d'années")
    parser.add_argument("--annee-min", type=int, default=ANNEE_MIN_DEFAULT)
    parser.add_argument("--annee-max", type=int, default=ANNEE_MAX_DEFAULT)
    parser.add_argument("--forcer-telechargement", action="store_true", help="re-télécharger même si déjà présent")
    return parser.parse_args()


# ---- main -------------------------------------------------------------------

def main():
    args = parse_args()

    ts_log = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = setup_logging(DIR_LOGS / f"pipeline_{ts_log}.log")

    logger.info("=" * 60)
    logger.info("  SGAE — Pipeline FTS")
    logger.info(f"  {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)

    annees = sorted(args.annees) if args.annees else list(range(args.annee_min, args.annee_max + 1))
    logger.info(f"  années : {annees}")

    # On fait le ménage au démarrage : processed + vieux logs, sauf le log qu'on vient d'ouvrir
    log_courant = Path(logger.handlers[1].stream.name).resolve()
    for dossier in [DIR_PROCESSED, DIR_LOGS]:
        if not dossier.exists():
            continue
        for f in dossier.iterdir():
            if not f.is_file() or f.resolve() == log_courant:
                continue
            try:
                f.unlink()
            except PermissionError:
                logger.warning(f"  impossible de supprimer {f.name} (fichier ouvert)")
    logger.info("  anciens fichiers supprimés")

    # Étape 1 — téléchargement
    fichiers = telecharger_tous_les_fichiers(annees, DIR_RAW, logger, forcer=args.forcer_telechargement)
    if not fichiers:
        logger.critical("aucun fichier disponible — vérifier la connexion")
        sys.exit(1)

    # Étape 2 — on re-consolide uniquement si le nombre d'années a changé
    DIR_PROCESSED.mkdir(parents=True, exist_ok=True)
    ETAT_FILE = DIR_PROCESSED / ".nb_fichiers_consolides"

    nb_fichiers  = len(fichiers)
    nb_precedent = int(ETAT_FILE.read_text()) if ETAT_FILE.exists() else -1
    fichier_existant = (
        next(DIR_PROCESSED.glob("*_FTS_DATASET_COMPLET.xlsx"), None)
        or next(DIR_PROCESSED.glob("*_FTS_DATASET_COMPLET.csv"), None)
    )

    besoin_consolidation = not (nb_fichiers == nb_precedent and fichier_existant)

    if besoin_consolidation:
        df = consolider(fichiers, logger)
        ETAT_FILE.write_text(str(nb_fichiers))
        logger.info("  suppression des fichiers bruts...")
        for f in DIR_RAW.glob("*.xlsx"):
            try:
                f.unlink()
            except PermissionError:
                logger.warning(f"  impossible de supprimer {f.name} (fichier ouvert)")
    else:
        logger.info(f"  pas de changement ({nb_fichiers} fichiers) — consolidation ignorée")
        logger.info(f"  chargement : {fichier_existant.name}")
        if fichier_existant.suffix == ".xlsx":
            df = pd.read_excel(fichier_existant, sheet_name="DATASET", dtype=str, engine="openpyxl")
        else:
            df = pd.read_csv(fichier_existant, dtype=str, sep=";", encoding="utf-8-sig", low_memory=False)
        logger.info(f"  {len(df):,} lignes chargées")

    # Procédure 1 — enrichissement (pays, CFP, NUTS)
    # On vérifie si c'est déjà fait pour ne pas re-tourner inutilement
    colonnes_enrichissement = {"Etat_Statut", "Periode_CFP", "CFP_Depense", "NUTS2_FR"}
    if colonnes_enrichissement.issubset(set(df.columns)):
        logger.info("  enrichissement déjà présent — ignoré")
    else:
        df = enrichir_procedure1(df, DIR_REFERENTIELS, logger)

    ts = datetime.now().strftime("%Y%m%d_%H%M")
    chemin_xlsx = DIR_PROCESSED / f"{ts}_FTS_DATASET_COMPLET.xlsx"

    if fichier_existant and fichier_existant.exists():
        try:
            fichier_existant.unlink()
        except PermissionError:
            logger.warning(f"  impossible de supprimer {fichier_existant.name} (fichier ouvert)")

    exporter_xlsx_colore(df, chemin_xlsx, logger)
    logger.info(f"  {len(df):,} lignes | colonnes : {list(df.columns)}")

    logger.info("=" * 60)
    logger.info(f"  terminé — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
