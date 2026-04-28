# Pipeline FTS — Financial Transparency System

Automatise la procédure CMFE de construction du fichier FTS enrichi.
Source : [ec.europa.eu/budget/financial-transparency-system](https://ec.europa.eu/budget/financial-transparency-system/)

## Ce que ça fait

| Étape | Description |
|---|---|
| 1 | Télécharge les fichiers XLSX annuels (2014–2024) depuis le portail CE |
| 2 | Empile les années en un seul dataset |
| Proc. 1 | Enrichit chaque ligne : statut pays, période CFP, localisation NUTS FR |
| Export | Produit un `.xlsx` avec les colonnes enrichies surlignées en jaune |

Les colonnes ajoutées (`Etat_Statut`, `Periode_CFP`, `CFP_Depense`, `NUTS2_FR`, `Region_FR`, `NUTS3_FR`) apparaissent en **fond jaune** dans le fichier de sortie et sont placées juste après le bénéficiaire.

## Installation

```bash
pip install requests pandas openpyxl xlsxwriter tqdm colorlog
```

## Utilisation

```bash
python fts_pipeline.py                          # toutes les années
python fts_pipeline.py --annees 2022 2023 2024  # années précises
python fts_pipeline.py --annee-min 2021         # à partir de 2021
python fts_pipeline.py --forcer-telechargement  # re-télécharger même si déjà présent
```

## Structure

```
FTS/
├── fts_pipeline.py          # script principal
├── fts_enrichissement.py    # procédure 1 : enrichissement + export coloré
├── referentiels/            # fichier XLSX de référence (pays, CFP, NUTS)
├── data/
│   ├── raw/                 # XLSX annuels (supprimés après consolidation)
│   └── processed/           # dataset enrichi (.xlsx)
└── logs/
```
