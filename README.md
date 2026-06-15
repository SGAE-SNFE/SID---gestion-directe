# SID---gestion-directe
Développements réalisés pour l'exploitation des données des fonds en gestion directe
```markdown
# SID - Gestion Directe : Recherche et Enrichissement de numéros TVA manquants

Ce dépôt contient un notebook Google Colab conçu pour identifier et rechercher les numéros de TVA français manquants dans des fichiers bruts FTS (Financial Transparency System) de la Commission européenne, en utilisant l'API `recherche-entreprises.api.gouv.fr`.

## 🚀 Objectif

À partir d'un fichier brut FTS (Excel ou CSV), le script a pour but de :
- Identifier les bénéficiaires français pour lesquels un numéro de TVA est manquant.
- Retrouver le numéro de TVA correspondant via l'API [Recherche d'Entreprises](https://api.gouv.fr/documentation/api-entreprise).
- Mettre à jour la colonne `VAT number of beneficiary` directement dans le fichier.
- Générer un fichier Excel enrichi avec un code couleur pour les lignes mises à jour (vert pour haute confiance, jaune pour confiance moyenne).
- Produire un rapport Excel détaillé de toutes les recherches effectuées, incluant le statut (trouvé, non trouvé, exclu), la stratégie utilisée, le SIREN, le TVA trouvé, le nom de l'API et un score détaillé.

## ✨ Fonctionnalités et Règles

- **Mise à jour en place** : Seule la colonne `VAT number of beneficiary` est modifiée.
- **Aucune colonne supplémentaire** : Le fichier de sortie conserve la structure originale sans ajout de colonnes.
- **Coloration des lignes** : Les lignes pour lesquelles un TVA a été trouvé sont colorées :
    - **Vert** : Score de haute confiance (>= 92).
    - **Jaune** : Score de confiance moyenne (entre 82 et 91).
- **Rapport détaillé** : Un rapport Excel séparé fournit une traçabilité complète de chaque tentative de recherche.

## 🔍 9 Stratégies de Recherche

Le script applique 9 stratégies de recherche en cascade pour maximiser les chances de trouver une correspondance :
1. Nom complet + filtre département
2. Nom complet sans filtre géographique
3. Nom principal (partie avant `*`)
4. Sigle/acronyme (partie après `*`)
5. Nom sans suffixes juridiques (SA, SAS, SARL, ASSO…)
6. Mots clés significatifs (4 premiers mots, sans mots vides)
7. Nom normalisé (sans accents, sans ponctuation)
8. Nom tronqué (pour les noms très longs)
9. Nom + ville

La recherche s'arrête dès qu'un numéro de TVA est trouvé avec un score suffisant (seuil d'acceptation de 82/100).

## 📊 Scoring sur 5 Critères (v2)

Un score de pertinence (entre 0 et 100) est calculé pour chaque résultat potentiel de l'API, basé sur 5 critères :

| Critère | Description | Effet | Détails |
|---|---|---|---|
| **① Similarité Textuelle** | `token_sort_ratio` + `token_set_ratio` + `partial_ratio` conditionnel | Score de base | Évalue la similarité des noms en tenant compte des permutations, inclusions et longueurs. |
| **② Ratio de Longueur** | Comparaison de la longueur des noms de référence et API | Pénalité −10% | Si les noms ont des longueurs très différentes (< 60%), `partial_ratio` est ignoré et une pénalité est appliquée. |
| **③ État Administratif** | Vérifie le statut `etat_administratif` de l'entreprise (champ 'C' pour cessée) | Pénalité −35% | Une entreprise radiée (SIREN fermé) ne peut pas avoir un TVA valide, ce qui pénalise fortement la correspondance. |
| **④ Validation Ville** | Comparaison de la ville du fichier FTS avec `siege.libelle_commune` de l'API | ±5 pts / ±7% | Bonus de +5 si similarité >= 80% ; malus de −7% si similarité < 35% et score actuel < 95. |
| **⑤ Validation Département** | Comparaison des 2 premiers chiffres du code postal FTS avec ceux de l'API | ±3 pts / ±4% | Bonus de +3 si même département ; malus de −4% si départements différents et score actuel < 93. |

La colonne `Detail_Score` dans le rapport Excel fournit un audit lisible de ces 5 critères pour chaque recherche.

## 🛠️ Utilisation du Notebook Google Colab

Pour exécuter ce script, ouvrez le fichier `.ipynb` dans Google Colab et suivez l'ordre d'exécution des cellules :

1.  **CELLULE 1 — Installation des dépendances** : Exécutez pour installer les bibliothèques nécessaires (`rapidfuzz`, `openpyxl`, `tqdm`, `requests`, etc.).
2.  **CELLULE 2 — Imports et configuration** : Charge les modules Python et configure les paramètres ajustables (seuils de score, délai API, etc.).
3.  **CELLULE 3 — Fonctions utilitaires, scoring et stratégies** : Définit les fonctions de normalisation, de conversion SIREN-TVA, les exclusions, la suppression de suffixes, l'extraction de mots-clés, la génération des requêtes API et le moteur de scoring.
4.  **CELLULE 4 — Enrichissement + détection des colonnes** : Contient les fonctions pour détecter automatiquement les colonnes pertinentes dans votre fichier d'entrée et la logique d'enrichissement principale.
5.  **CELLULE 5 — Fonctions d'export (fichier coloré + rapport)** : Contient les fonctions pour exporter le fichier Excel enrichi avec la coloration conditionnelle et générer le rapport détaillé.
6.  **CELLULE 6 — Upload du fichier brut FTS** : Cette cellule vous demandera de télécharger votre fichier Excel ou CSV brut depuis votre ordinateur.
7.  **CELLULE 7 — Enrichissement TVA + export des résultats** : Lance le processus de recherche des TVA manquants et génère les fichiers de sortie (fichier enrichi et rapport).

Après l'exécution de la **Cellule 7**, les fichiers `_TVA_ENRICHI.xlsx` et `_RAPPORT_TVA.xlsx` seront automatiquement téléchargés sur votre machine.

## ⚙️ Paramètres Ajustables

Dans la **CELLULE 2**, vous pouvez ajuster les paramètres suivants :

- `SEUIL_SCORE` : Score minimal (0-100) pour accepter un numéro de TVA trouvé (par défaut : 82).
- `SEUIL_HAUTE_CONF` : Score à partir duquel une correspondance est considérée comme de 'haute confiance' (par défaut : 92) et colorée en vert.
- `DELAI_API` : Délai en secondes entre deux appels à l'API pour respecter les limites de débit (par défaut : 0.15s, correspondant à 7 requêtes/seconde).
- `MAX_PAR_REQUETE` : Nombre maximum de résultats API à comparer par requête (par défaut : 5).

```
