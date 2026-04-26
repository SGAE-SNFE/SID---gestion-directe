# Pipeline FTS — Financial Transparency System

Script Python qui automatise la récupération et la consolidation des données du [Financial Transparency System (FTS)](https://ec.europa.eu/budget/financial-transparency-system/) de la Commission européenne.

## Ce que fait le script

**Étape 1 — Téléchargement**
Récupère les fichiers XLSX annuels (2014–2024) directement depuis `ec.europa.eu`. Si un fichier est déjà présent et valide sur le disque, il n'est pas retéléchargé.

**Étape 2 — Consolidation**
Empile les 11 fichiers annuels les uns sous les autres pour produire un seul fichier `FTS_DATASET_COMPLET.csv`. La consolidation est ignorée si le nombre de fichiers sources n'a pas changé depuis le dernier lancement.

Après consolidation, les fichiers XLSX bruts sont supprimés pour libérer de l'espace.

## Installation

```bash
pip install requests pandas openpyxl tqdm colorlog
```

## Utilisation

```bash
# toutes les années (2014–2024)
python fts_pipeline.py

# années précises
python fts_pipeline.py --annees 2022 2023 2024

# à partir d'une année
python fts_pipeline.py --annee-min 2021

# forcer le re-téléchargement même si les fichiers sont déjà présents
python fts_pipeline.py --forcer-telechargement
```

## Structure du projet

```
FTS/
├── fts_pipeline.py          # script principal
├── data/
│   ├── raw/                 # fichiers XLSX téléchargés (supprimés après consolidation)
│   └── processed/           # dataset consolidé (CSV)
└── logs/                    # logs d'exécution
```

## Source des données

Les données proviennent du portail officiel de la Commission européenne :
[https://ec.europa.eu/budget/financial-transparency-system/](https://ec.europa.eu/budget/financial-transparency-system/)
