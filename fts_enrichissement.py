# Procédure 1 — Enrichissement du dataset FTS consolidé
#
# Ce module prend le dataset brut (lignes empilées) et ajoute 6 colonnes :
#   - Etat_Statut  : FR / UE / UK / AELE / Autre  (depuis l'onglet ETATS)
#   - Periode_CFP  : 14-20 / 21-27 / Hors CFP     (depuis BUDGET LINE_CFP)
#   - CFP_Depense  : 2014-2020 / 2021-2027 / …     (idem)
#   - NUTS2_FR     : région NUTS2 pour les bénéficiaires français
#   - Region_FR    : région administrative française
#   - NUTS3_FR     : département
#
# Les deux premières (Etat_Statut, Periode_CFP) sont glissées juste après
# la colonne "Name of beneficiary" pour rester cohérent avec la procédure CMFE.
# Dans le fichier Excel de sortie, toutes les colonnes ajoutées apparaissent en jaune.

import logging
from pathlib import Path

import pandas as pd


# Liste des colonnes qu'on ajoute — utilisée pour le surlignage jaune à l'export
NOUVELLES_COLONNES = ["Etat_Statut", "Periode_CFP", "CFP_Depense", "NUTS2_FR", "Region_FR", "NUTS3_FR"]


# ---- utilitaire de recherche de colonne -------------------------------------

def _col(df: pd.DataFrame, mots: list[str]) -> str | None:
    # Cherche la première colonne dont le nom contient un des mots-clés (sans tenir compte de la casse)
    for mot in mots:
        for c in df.columns:
            if mot.lower() in c.lower():
                return c
    return None


# ---- chargement des référentiels --------------------------------------------

def trouver_referentiel(dossier: Path) -> Path | None:
    # On prend le premier xlsx trouvé dans le dossier — le nom du fichier peut changer (préfixe date)
    fichiers = list(dossier.glob("*.xlsx"))
    return fichiers[0] if fichiers else None


def charger_referentiels(chemin: Path, logger: logging.Logger) -> dict | None:
    logger.info(f"  référentiel : {chemin.name}")
    try:
        xl = pd.ExcelFile(chemin)
    except Exception as e:
        logger.error(f"  impossible d'ouvrir le référentiel : {e}")
        return None

    # Onglet ETATS : code pays → statut (FR / UE / UK / AELE)
    ref_etats = pd.read_excel(xl, sheet_name="ETATS", dtype=str)
    ref_etats = ref_etats[["Etats_long_EN", "Etats_court", "Etat_Statut"]].dropna()

    # Onglet BUDGET LINE_CFP : numéro de ligne budgétaire → période CFP + dépense
    ref_cfp = pd.read_excel(xl, sheet_name="BUDGET LINE_CFP", dtype=str)
    col_bl  = _col(ref_cfp, ["budget line number"])
    col_cfp = _col(ref_cfp, ["riode", "Periode"])   # "Période CFP" — accent parfois absent
    col_dep = _col(ref_cfp, ["pense", "Depense"])   # "CFP Dépense"
    if not all([col_bl, col_cfp, col_dep]):
        logger.error("  onglet BUDGET LINE_CFP : colonnes introuvables")
        return None
    ref_cfp = (
        ref_cfp[[col_bl, col_cfp, col_dep]]
        .rename(columns={col_bl: "bl_num", col_cfp: "Periode_CFP", col_dep: "CFP_Depense"})
        .dropna(subset=["bl_num"])
    )

    # Onglet NUTS 2-3 FR : numéro de département → région NUTS2, région admin, département NUTS3
    ref_nuts = pd.read_excel(xl, sheet_name="NUTS 2-3 FR")
    col_num = next((c for c in ref_nuts.columns if "Num" in c), None)    # "NUTS3_Numéro"
    col_reg = next((c for c in ref_nuts.columns if "gion" in c), None)   # "Région_FR"
    if not col_num or not col_reg:
        logger.error("  onglet NUTS 2-3 FR : colonnes introuvables")
        return None
    ref_nuts = (
        ref_nuts[["NUTS2_FR", col_reg, "NUTS3_FR", col_num]]
        .rename(columns={col_reg: "Region_FR", col_num: "dept_num"})
        .dropna(subset=["dept_num"])
    )
    ref_nuts["dept_num"] = pd.to_numeric(ref_nuts["dept_num"], errors="coerce")
    ref_nuts = ref_nuts.dropna(subset=["dept_num"])
    ref_nuts["dept_num"] = ref_nuts["dept_num"].astype(int)
    # Certains départements apparaissent plusieurs fois (alias NUTS2) — on garde le premier
    ref_nuts = ref_nuts.drop_duplicates(subset=["dept_num"]).set_index("dept_num")

    logger.info(
        f"  {len(ref_etats)} pays | {len(ref_cfp)} lignes budgétaires | {len(ref_nuts)} départements"
    )
    return {"etats": ref_etats, "cfp": ref_cfp, "nuts": ref_nuts}


# ---- enrichissements --------------------------------------------------------

def _statut_pays(df: pd.DataFrame, ref: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    col = _col(df, ["country", "pays"])
    if col is None:
        logger.warning("  [Proc1] colonne pays introuvable — Etat_Statut ignoré")
        return df

    # Selon les années, le FTS peut contenir des noms complets ("France") ou des codes ("FR")
    sample_len = df[col].dropna().str.strip().str.len().median()
    if sample_len <= 3:
        pays_map = dict(zip(ref["Etats_court"], ref["Etat_Statut"]))
    else:
        pays_map = dict(zip(ref["Etats_long_EN"], ref["Etat_Statut"]))

    df["Etat_Statut"] = df[col].str.strip().map(pays_map).fillna("Autre")
    logger.info(
        f"  [pays] FR={(df['Etat_Statut']=='FR').sum():,}"
        f" | UE={(df['Etat_Statut']=='UE').sum():,}"
        f" | Autre={(df['Etat_Statut']=='Autre').sum():,}"
    )
    return df


def _periode_cfp(df: pd.DataFrame, ref: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    col_bl   = _col(df, ["budget line number"])
    col_year = _col(df, ["year"])

    cfp_map = dict(zip(ref["bl_num"], ref["Periode_CFP"]))
    dep_map = dict(zip(ref["bl_num"], ref["CFP_Depense"]))

    if col_bl:
        df["Periode_CFP"] = df[col_bl].str.strip().map(cfp_map)
        df["CFP_Depense"]  = df[col_bl].str.strip().map(dep_map)
    else:
        df["Periode_CFP"] = pd.NA
        df["CFP_Depense"]  = pd.NA
        logger.warning("  [Proc1] colonne 'budget line number' introuvable")

    # Pour les lignes qui n'ont pas matchées (budget line non répertoriée), on tombe back sur l'année
    if col_year:
        mask = df["Periode_CFP"].isna()
        df.loc[mask, "Periode_CFP"] = df.loc[mask, col_year].apply(
            lambda y: "14-20" if str(y) < "2021" else "21-27"
        )
        mask2 = df["CFP_Depense"].isna()
        df.loc[mask2, "CFP_Depense"] = df.loc[mask2, col_year].apply(
            lambda y: "2014-2020" if str(y) < "2021" else "2021-2027"
        )

    nb_ok = df["Periode_CFP"].notna().sum()
    logger.info(f"  [CFP] {nb_ok:,}/{len(df):,} lignes qualifiées")
    return df


def _dept_depuis_cp(cp) -> int | None:
    # Les codes postaux français DOM-TOM commencent par 97x ou 98x — on prend 3 chiffres dans ce cas
    if pd.isna(cp):
        return None
    pc = str(cp).strip().replace(" ", "").zfill(5)
    try:
        prefix = pc[:3] if pc[:2] in ("97", "98") else pc[:2]
        return int(prefix)
    except (ValueError, IndexError):
        return None


def _nuts_fr(df: pd.DataFrame, nuts_map: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    col_cp   = _col(df, ["postal", "zip", "code postal"])
    col_pays = _col(df, ["country", "pays"])
    if col_cp is None or col_pays is None:
        logger.warning("  [Proc1] colonnes CP/pays introuvables — NUTS ignoré")
        return df

    df["NUTS2_FR"]  = pd.NA
    df["Region_FR"] = pd.NA
    df["NUTS3_FR"]  = pd.NA

    # On utilise Etat_Statut si on vient de le calculer — sinon on filtre sur le nom
    if "Etat_Statut" in df.columns:
        mask_fr = df["Etat_Statut"] == "FR"
    else:
        mask_fr = df[col_pays].str.strip().isin(["France", "FR"])

    depts   = df.loc[mask_fr, col_cp].apply(_dept_depuis_cp)
    trouvés = 0
    for idx, dept in depts.items():
        if dept is not None and dept in nuts_map.index:
            r = nuts_map.loc[dept]
            df.at[idx, "NUTS2_FR"]  = r["NUTS2_FR"]
            df.at[idx, "Region_FR"] = r["Region_FR"]
            df.at[idx, "NUTS3_FR"]  = r["NUTS3_FR"]
            trouvés += 1

    logger.info(f"  [NUTS] {trouvés:,}/{mask_fr.sum():,} bénéficiaires FR localisés")
    return df


# ---- ordre des colonnes -----------------------------------------------------

def reordonner_colonnes(df: pd.DataFrame) -> pd.DataFrame:
    # Etat_Statut et Periode_CFP doivent apparaître juste après le nom du bénéficiaire
    # pour rester cohérent avec la présentation de la procédure CMFE
    cols = list(df.columns)
    a_inserer = [c for c in ["Etat_Statut", "Periode_CFP"] if c in cols]
    if not a_inserer:
        return df

    col_benef = _col(df, ["name of beneficiary", "beneficiary name", "beneficiary"])
    for c in a_inserer:
        cols.remove(c)

    pos = cols.index(col_benef) + 1 if (col_benef and col_benef in cols) else len(cols)
    for i, c in enumerate(a_inserer):
        cols.insert(pos + i, c)

    return df[cols]


# ---- export -----------------------------------------------------------------

def exporter_xlsx_colore(df: pd.DataFrame, chemin: Path, logger: logging.Logger) -> None:
    # On utilise xlsxwriter (plus rapide qu'openpyxl pour l'écriture).
    # Le fond jaune sur les données est un format conditionnel "=TRUE" — Python écrit
    # juste une règle XML par colonne, c'est Excel qui l'applique à l'ouverture du fichier.
    # Ça évite de formater 952 000 cellules une par une, ce qui serait très lent.
    logger.info(f"  export Excel : {chemin.name}")
    n = len(df)

    with pd.ExcelWriter(chemin, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="DATASET")
        wb = writer.book
        ws = writer.sheets["DATASET"]

        fmt_hdr_jaune  = wb.add_format({"bg_color": "#FFD700", "bold": True, "border": 2, "text_wrap": True, "valign": "vcenter"})
        fmt_hdr_normal = wb.add_format({"bold": True, "border": 1, "bg_color": "#D9D9D9"})
        fmt_data_jaune = wb.add_format({"bg_color": "#FFFACD"})

        for i, col in enumerate(df.columns):
            if col in NOUVELLES_COLONNES:
                ws.write(0, i, col, fmt_hdr_jaune)
                ws.conditional_format(1, i, n, i, {
                    "type": "formula",
                    "criteria": "=TRUE",
                    "format": fmt_data_jaune,
                })
            else:
                ws.write(0, i, col, fmt_hdr_normal)

        ws.set_row(0, 30)
        ws.freeze_panes(1, 0)   # ligne d'en-tête figée

    logger.info(f"  {n:,} lignes exportées → {chemin.name}")


# ---- point d'entrée ---------------------------------------------------------

def enrichir_procedure1(df: pd.DataFrame, dossier_ref: Path, logger: logging.Logger) -> pd.DataFrame:
    logger.info("=" * 60)
    logger.info("PROCÉDURE 1 — Enrichissement")
    logger.info("=" * 60)

    chemin = trouver_referentiel(dossier_ref)
    if chemin is None:
        logger.warning(f"  aucun fichier XLSX dans {dossier_ref} — enrichissement ignoré")
        return df

    refs = charger_referentiels(chemin, logger)
    if refs is None:
        return df

    df = _statut_pays(df, refs["etats"], logger)
    df = _periode_cfp(df, refs["cfp"], logger)
    df = _nuts_fr(df, refs["nuts"], logger)
    df = reordonner_colonnes(df)

    logger.info("  enrichissement terminé")
    return df
