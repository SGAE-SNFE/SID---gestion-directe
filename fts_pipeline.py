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


# pour l'instant on fait juste les etapes 1 et 2 de la procedure CMFE :
# telecharger les fichiers FTS annuels et les coller les uns sous les autres
# le reste (pays, CFP, fichier france...) on le fera apres

# ---- URLs et headers --------------------------------------------------------

# url trouvee via F12 dans le navigateur (l'ancienne avec /document/ ne marche plus)
FTS_BASE_URL = (
    "https://ec.europa.eu/budget/financial-transparency-system/download/"
    "{annee}_FTS_dataset_en.xlsx"
)

# url de secours au cas ou la premiere ne repond pas
FTS_ALT_URL = (
    "https://commission.europa.eu/document/"
    "{annee}_FTS_dataset_en.xlsx"
)

# sans ces headers le serveur renvoie une page html au lieu du fichier
# j'ai du ajouter le Referer et le User-Agent pour que ca marche
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

# annees disponibles sur le site FTS
ANNEE_MIN_DEFAULT = 2014
ANNEE_MAX_DEFAULT = 2024

# dossiers utilises par le script
DIR_RAW       = Path("data/raw")        # les xlsx bruts telecharges
DIR_PROCESSED = Path("data/processed")  # le csv consolide
DIR_LOGS      = Path("logs")


# ---- logs -------------------------------------------------------------------

def setup_logging(log_file: Path) -> logging.Logger:
    DIR_LOGS.mkdir(parents=True, exist_ok=True)

    # affichage couleur dans le terminal
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

    # copie dans un fichier pour garder une trace
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


# ---- etape 1 : telechargement -----------------------------------------------

def _est_xlsx_valide(chemin: Path) -> bool:
    # un vrai fichier xlsx commence toujours par "PK" (c'est un zip renomme)
    # si on trouve autre chose c'est que le serveur a renvoye du html
    try:
        with open(chemin, "rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False


def telecharger_fichier_fts(annee: int, repertoire: Path, logger: logging.Logger, forcer: bool = False) -> Path | None:
    repertoire.mkdir(parents=True, exist_ok=True)
    chemin_local = repertoire / f"{annee}_FTS_dataset_en.xlsx"

    # si le fichier est deja la et valide, pas besoin de le retelecharger
    if chemin_local.exists() and not forcer:
        if _est_xlsx_valide(chemin_local):
            taille_mo = chemin_local.stat().st_size / 1024 ** 2
            logger.info(f"  [{annee}] deja present ({taille_mo:.1f} Mo) — on passe")
            return chemin_local
        else:
            # le fichier existe mais c'est du html, probablement un telechargement rate
            logger.warning(f"  [{annee}] fichier invalide sur le disque — suppression et retry")
            try:
                chemin_local.unlink()
            except PermissionError:
                logger.error(f"  [{annee}] impossible de supprimer {chemin_local.name} — ferme le fichier et relance")
                return None

    # on essaie les deux urls l'une apres l'autre
    for url in [FTS_BASE_URL.format(annee=annee), FTS_ALT_URL.format(annee=annee)]:
        logger.info(f"  [{annee}] essai : {url}")
        try:
            response = requests.get(url, timeout=180, stream=True, headers=FTS_HEADERS)

            if response.status_code != 200:
                logger.warning(f"  [{annee}] HTTP {response.status_code} — on essaie l'url suivante")
                time.sleep(2)
                continue

            # si c'est du html le serveur nous a renvoye une page web au lieu du fichier
            content_type = response.headers.get("content-type", "")
            if "html" in content_type.lower():
                logger.warning(f"  [{annee}] reponse HTML recue ({content_type}) — url probablement obsolete")
                time.sleep(2)
                continue

            # telechargement avec barre de progression
            taille_attendue = int(response.headers.get("content-length", 0))
            with open(chemin_local, "wb") as f:
                with tqdm(total=taille_attendue, unit="B", unit_scale=True, desc=f"    {annee}", leave=False) as barre:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        barre.update(len(chunk))

            # verification que ce qu'on a recu est bien un xlsx et pas du html
            if not _est_xlsx_valide(chemin_local):
                logger.warning(f"  [{annee}] fichier recu invalide — supprime")
                chemin_local.unlink()
                time.sleep(2)
                continue

            taille_reelle = chemin_local.stat().st_size
            logger.info(f"  [{annee}] ok — {taille_reelle / 1024:.0f} Ko")
            return chemin_local

        except requests.Timeout:
            logger.warning(f"  [{annee}] timeout sur {url}")
        except requests.RequestException as e:
            logger.warning(f"  [{annee}] erreur reseau : {e}")

        time.sleep(2)

    logger.error(f"  [{annee}] echec — aucune url n'a fonctionne")
    logger.error(f"  [{annee}] verifier manuellement : https://ec.europa.eu/budget/financial-transparency-system/")
    return None


def telecharger_tous_les_fichiers(annees: list, repertoire: Path, logger: logging.Logger, forcer: bool = False) -> dict:
    logger.info("=" * 60)
    logger.info(f"ETAPE 1 — Telechargement ({len(annees)} fichiers : {annees[0]}-{annees[-1]})")
    logger.info("=" * 60)

    resultats = {}
    for annee in annees:
        chemin = telecharger_fichier_fts(annee, repertoire, logger, forcer)
        if chemin:
            resultats[annee] = chemin

    logger.info(f"  {len(resultats)}/{len(annees)} fichiers disponibles")
    return resultats


# ---- etape 2 : consolidation ------------------------------------------------
# c'est l'equivalent du copier-coller manuel dans l'onglet DATASET
# on empile juste les lignes de chaque annee les unes sous les autres, rien de plus

def charger_fichier_fts(chemin: Path, annee: int, logger: logging.Logger) -> pd.DataFrame | None:
    # on lit tout en texte (dtype=str) pour ne pas que pandas transforme les valeurs
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
    logger.info("ETAPE 2 — Consolidation")
    logger.info("=" * 60)

    frames = []
    for annee in sorted(fichiers.keys()):
        df = charger_fichier_fts(fichiers[annee], annee, logger)
        if df is not None:
            frames.append(df)

    if not frames:
        logger.critical("aucun fichier charge — arret")
        sys.exit(1)

    df_total = pd.concat(frames, ignore_index=True)
    logger.info(f"  consolide : {len(df_total):,} lignes au total | annees {sorted(fichiers.keys())}")
    return df_total


# ---- arguments --------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="telechargement + consolidation des fichiers FTS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
exemples :
  python fts_pipeline.py                          -> toutes les annees
  python fts_pipeline.py --annees 2022 2023 2024  -> annees precises
  python fts_pipeline.py --annee-min 2021         -> a partir de 2021
  python fts_pipeline.py --forcer-telechargement  -> retelecharger meme si deja present
        """
    )
    parser.add_argument("--annees", nargs="+", type=int, help="liste d'annees")
    parser.add_argument("--annee-min", type=int, default=ANNEE_MIN_DEFAULT)
    parser.add_argument("--annee-max", type=int, default=ANNEE_MAX_DEFAULT)
    parser.add_argument("--forcer-telechargement", action="store_true", help="retelecharger meme si deja present")
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
    logger.info(f"  annees : {annees}")

    # nettoyage des anciens fichiers avant de commencer (processed + vieux logs)
    # on garde uniquement le log qu'on vient d'ouvrir
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
    logger.info("  anciens fichiers supprimes")

    # etape 1
    fichiers = telecharger_tous_les_fichiers(annees, DIR_RAW, logger, forcer=args.forcer_telechargement)
    if not fichiers:
        logger.critical("aucun fichier disponible — verifier la connexion")
        sys.exit(1)

    # etape 2 — on consolide seulement si le nombre de fichiers a change
    # sinon on garde le csv deja produit
    DIR_PROCESSED.mkdir(parents=True, exist_ok=True)
    ETAT_FILE = DIR_PROCESSED / ".nb_fichiers_consolides"

    nb_fichiers  = len(fichiers)
    nb_precedent = int(ETAT_FILE.read_text()) if ETAT_FILE.exists() else -1
    csv_existant = next(DIR_PROCESSED.glob("*_FTS_DATASET_COMPLET.csv"), None)

    if nb_fichiers == nb_precedent and csv_existant:
        logger.info(f"  pas de changement ({nb_fichiers} fichiers) — consolidation ignoree")
        logger.info(f"  fichier existant : {csv_existant.name}")
    else:
        df = consolider(fichiers, logger)

        ts = datetime.now().strftime("%Y%m%d_%H%M")
        chemin_csv = DIR_PROCESSED / f"{ts}_FTS_DATASET_COMPLET.csv"

        df.to_csv(chemin_csv, index=False, encoding="utf-8-sig", sep=";")
        logger.info(f"  csv exporte : {chemin_csv.name}")
        logger.info(f"  {len(df):,} lignes | colonnes : {list(df.columns)}")

        # on retient combien de fichiers ont ete consolides
        ETAT_FILE.write_text(str(nb_fichiers))

        # suppression des xlsx bruts pour liberer de la place
        logger.info("  suppression des fichiers bruts...")
        for f in DIR_RAW.glob("*.xlsx"):
            try:
                f.unlink()
            except PermissionError:
                logger.warning(f"  impossible de supprimer {f.name} (fichier ouvert)")

    logger.info("=" * 60)
    logger.info(f"  termine — {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
