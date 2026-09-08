# Croisement FTS × Kohesio — captation des fonds européens

Rapprochement des financements européens perçus par les acteurs français, en
**gestion directe** (FTS – Financial Transparency System) et en **gestion
partagée** (Kohesio – FEDER, FSE+, FTJ).

Travaux réalisés pour le pôle « Mobilisation des fonds européens » du SGAE.

## Objectif

Mesurer, pour chaque bénéficiaire et chaque année, ce qu'il capte sur chacun des
deux canaux, et produire un fichier exploitable dans MicroStrategy.

## Contenu

| Notebook | Rôle |
|---|---|
| `KOHESIO_beneficiaires_COMPLET_v6.ipynb` | Siretisation des bénéficiaires Kohesio via l'API SIRENE (moteur FTS réutilisé) |
| `REPARTITION_CANAUX.ipynb` | Croisement des deux bases et calcul de la répartition par canal |

## Principes de traitement

- **Clé de rapprochement : le SIREN**, manipulé en texte (les zéros de tête sont
  significatifs). Le nom ne peut pas servir de clé : le CNRS apparaît sous 9
  libellés dans Kohesio.
- **Nom de référence : `Nom API`**, le nom légal issu de SIRENE, identique dans
  les deux bases.
- **Assiettes** : `Beneficiary contracted amount` (FTS) et `Project_EU_Budget`
  (Kohesio) — deux contributions européennes, donc comparables.
- **Les deux canaux ne sont pas additionnables** : le FTS porte des engagements
  annuels, Kohesio des montants programmés sur toute la durée du projet. Le
  total produit est explicitement *indicatif* et sert à hiérarchiser, non à
  produire un montant comptable.
- **Année** : exercice budgétaire côté FTS ; année de début de financement côté
  Kohesio. La répartition sur la période complète est plus robuste que
  l'annuelle.

## Sorties

`REPARTITION_CANAUX_MICROSTRATEGY.xlsx`, deux feuilles :

- `POUR_TABLEAU_large` — une ligne par SIREN × année, montants en colonnes
- `POUR_GRAPHIQUE_long` — une ligne par SIREN × année × canal, pour les
  graphiques empilés

## Limites connues

- Les lignes non siretisées sont exclues du croisement ; le taux de siretisation
  est affiché à l'exécution et doit être vérifié avant interprétation.
- La géolocalisation de Kohesio est erronée sur environ 12 % des cas (homonymes
  de communes) et n'est pas utilisée.
- L'annualisation des montants Kohesio est une convention, pas une consommation
  réelle.

## Exécution

Conçu pour Google Colab. Ouvrir le notebook, exécuter les cellules dans l'ordre,
déposer les fichiers demandés.
