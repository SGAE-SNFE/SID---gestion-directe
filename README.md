# README — Pipeline FTS France (v5_41)

> Document mis à jour pour la version **v5_41** du notebook
> `FTS_France_PREPARATION_ENRICHISSEMENT`. Les nouveautés depuis la version
> précédemment documentée (v5_19) sont récapitulées en [section 17](#17-nouveautés-depuis-la-v5_19).

## Table des matières

1. [À quoi sert ce pipeline](#1-à-quoi-sert-ce-pipeline)
2. [Données d'entrée et de sortie](#2-données-dentrée-et-de-sortie)
3. [Installation et exécution](#3-installation-et-exécution)
4. [Architecture du notebook](#4-architecture-du-notebook)
5. [**Le traitement GLOBAL (Étape 0)**](#5-le-traitement-global-étape-0--toutes-lignes-tous-pays)
6. [**Le moteur de recherche : chaque stratégie en détail**](#6-le-moteur-de-recherche--chaque-stratégie-en-détail)
7. [Les règles intangibles](#7-les-règles-intangibles)
8. [Les enrichissements France, étape par étape](#8-les-enrichissements-étape-par-étape)
9. [Seuils, couleurs et conventions de sortie](#9-seuils-couleurs-et-conventions-de-sortie)
10. [**Forcer manuellement un SIREN, un SIRET ou une TVA**](#10-forcer-manuellement-un-siren-un-siret-ou-une-tva)
11. [Ajouter ou modifier un référentiel](#11-ajouter-ou-modifier-un-référentiel)
12. [Le cycle de correction par le rapport](#12-le-cycle-de-correction-par-le-rapport)
13. [Tester une modification sans rien casser](#13-tester-une-modification-sans-rien-casser)
14. [Dépannage](#14-dépannage)
15. [Glossaire](#15-glossaire)
16. [Sources et références](#16-sources-et-références)
17. [Nouveautés depuis la v5_19](#17-nouveautés-depuis-la-v5_19)

---

## 1. À quoi sert ce pipeline

### 1.1 Le problème

Le **FTS** (*Financial Transparency System*) est la base publique par laquelle la
Commission européenne publie les bénéficiaires des fonds de l'Union exécutés en
gestion **directe** et **indirecte**. Ce fichier est une matière première
précieuse pour piloter la performance française d'accès aux financements
européens — mais il est difficilement exploitable en l'état :

- le **numéro de TVA** est absent sur une grande partie des lignes ;
- il n'y a **ni SIREN ni SIRET** avant le millésime 2025, donc aucun moyen direct
  de relier un bénéficiaire aux référentiels français (INSEE, opérateurs de
  l'État) ;
- l'**adresse est fréquemment masquée** par des caractères de remplacement
  (`.`, `-`, `*****`), ou rédigée à l'anglo-saxonne (voie puis numéro), ce qui
  empêche toute ventilation territoriale ;
- la **nomenclature géographique** (NUTS2) est lacunaire et exprimée en
  anciennes régions ;
- les **libellés de bénéficiaires** comportent doublons, variantes
  orthographiques, sigles et traductions ;
- les colonnes d'**identification de projet** (`Call for proposals Reference`,
  `Project / Contract Reference`, `Project / Contract Acronym`) ne sont
  renseignées **qu'à partir du millésime 2025** : avant, elles valent
  `N/A - Not applicable` alors que l'information figure dans le libellé de
  l'objet.

Avant automatisation, ce travail d'identification était réalisé à la main et
mobilisait **plusieurs semaines** par livraison, avec un risque d'erreur et
d'irrégularité élevé.

### 1.2 Ce que fait le pipeline

À partir du fichier FTS brut, le notebook produit une donnée **identifiée,
qualifiée et territorialisée**, prête à être chargée dans MicroStrategy :

| Apport | Détail |
|---|---|
| **Identification** | numéro de TVA, SIREN, SIRET retrouvés par appariement automatique avec la base SIRENE, ou repris du champ *Main Registration* de la Commission quand il est présent |
| **Nom officiel** | `Nom API` : écriture exacte du bénéficiaire dans l'annuaire des entreprises, à comparer au libellé FTS |
| **Qualification juridique** | forme juridique (3 niveaux INSEE), statut simplifié SGAE, code et libellé NAF, section NAF 2025, état d'activité |
| **Qualification institutionnelle** | opérateur de l'État (avec **période de validité**) et programme budgétaire chef de file |
| **Territorialisation** | adresse réordonnée, code postal corrigé, NUTS2, Région, NUTS3, statut Metro / RUP / PTOM |
| **Identification de projet** | `Référence projet complétée`, dérivée du libellé de l'objet quand la Commission ne la fournit pas |
| **Mémoire inter-millésimes** | les SIREN certains d'un millésime sont réutilisés sur les autres, sans nouvel appel réseau |
| **Traçabilité** | rapport d'audit exhaustif avec score, stratégie gagnante et code couleur |

---

## 2. Données d'entrée et de sortie

### 2.1 Fichier d'entrée

Un export FTS au format `.xlsx` ou `.csv`. Les colonnes sont détectées par
**mots-clés** et non par nom exact, ce qui rend le pipeline tolérant aux
variations d'en-têtes d'une livraison à l'autre.

| Rôle | Mots-clés reconnus | Nécessité |
|---|---|---|
| Nom du bénéficiaire | `name of beneficiary`, `nom benefi`… | **obligatoire** |
| Pays | `beneficiary country`, `country`, `pays` | **obligatoire** |
| Numéro de TVA | `vat number`, `tva` | fortement recommandé |
| **Main registration number** | `main registration` | **millésime 2025+** — évite une recherche |
| Adresse | `address`, `adresse`, `street` | améliore le score |
| Ville | `city`, `ville` | améliore le score |
| Code postal | `postal code`, `code postal`, `zip` | améliore fortement le score |
| NUTS2 | `nuts2` | utile |
| Montant | `contracted amount` | requis pour la règle « A CHERCHER » |
| **Objet du contrat** | `subject of grant` | source de la `Référence projet complétée` |

> **Un export FTS ne contient qu'un seul millésime** (vérifié : le fichier 2025
> ne porte que `Year = 2025`). C'est ce qui justifie le mécanisme de mémoire
> décrit au §8.

**Fichier optionnel à déposer avec le FTS** : `correspondance_siren.json`,
produit par une exécution antérieure (voir §2.2). Sa présence n'est pas
nécessaire — un socle de correspondances est embarqué en dur dans le notebook —
mais elle enrichit la mémoire des exécutions précédentes.

### 2.2 Fichiers de sortie

| Fichier | Contenu | Usage |
|---|---|---|
| `<nom>_GLOBAL.xlsx` | toutes les lignes, tous pays, enrichissement transversal (zone UE/AELE/UK, période CFP, référence projet) | vue d'ensemble |
| `<nom>_FRANCE.xlsx` | lignes françaises **entièrement enrichies** ; ne contient que les TVA de score ≥ 96 | vue FRANCE |
| `<nom>_RAPPORT_TVA.xlsx` | **toutes** les recherches, y compris les échecs, avec score et stratégie | audit, correction manuelle |
| `correspondance_siren.json` | mémoire des couples (nom, département) → SIREN certains | à redéposer au traitement du millésime suivant |

> Le rapport n'est **pas** créé si aucune TVA n'était manquante : c'est normal,
> l'orchestration vérifie l'existence du fichier avant de le télécharger.

---

## 3. Installation et exécution

### 3.1 Environnement recommandé : Google Colab

Le notebook est conçu pour **Google Colab** et n'exige aucune installation
locale. Il détecte automatiquement son environnement (`IN_COLAB`).

```
1. Ouvrir https://colab.research.google.com
2. Fichier > Importer un notebook > déposer le .ipynb
3. Exécuter les cellules DANS L'ORDRE, de haut en bas
```

### 3.2 Ordre d'exécution

| Ordre | Cellule | Rôle |
|---|---|---|
| 1 | 1 | installation des paquets |
| 2 | 2 | référentiels embarqués (aucune exécution lourde) |
| 3 | 3 | imports et paramètres |
| 4 | 4 à 11 | définition des fonctions |
| 5 | 13 | chargement des référentiels **puis dépôt du fichier FTS** |
| 6 | 15 | **orchestration : le traitement complet** |

### 3.3 Durée et facteur limitant

La durée est dominée par les appels à l'API publique, cadencés par
`DELAI_API = 0.15 s`. Sur un fichier volumineux, comptez **plusieurs heures**.

> Sur un export **2025**, la Passe 0 (*Main Registration*) et la Passe mémoire
> évitent la recherche pour une grande partie des lignes : la durée réelle est
> nettement inférieure à celle d'un millésime ancien de même volume.

### 3.4 Exécution locale (Jupyter)

Le notebook fonctionne aussi en local. `IN_COLAB` passe à `False` : le dépôt de
fichier par widget est remplacé par une lecture de chemin, et les fichiers de
sortie sont écrits sur disque au lieu d'être téléchargés.

```bash
pip install rapidfuzz openpyxl xlsxwriter tqdm requests ipywidgets pycountry
jupyter notebook
```

---

## 4. Architecture du notebook

### 4.1 Vue d'ensemble

```
┌─ CELLULE 1 ──── installation des paquets
├─ CELLULE 2 ──── RÉFÉRENTIELS EMBARQUÉS (blocs JSON en dur, ~900 Ko)
├─ CELLULE 3 ──── imports, seuils, couleurs, détection Colab
├─ CELLULE 4 ──── MOTEUR DE RECHERCHE  ← 5 fonctions FIGÉES
│                 + enveloppe rechercher_tva_plus (modifiable)
│                 + tables de forçage manuel (§10)
├─ CELLULE 5 ──── Étape A : enrichissement TVA (Passes 0, mémoire, 1, 2)
├─ CELLULE 6 ──── export du rapport
├─ CELLULE 7 ──── Étape A3 : complétion géographique SIRENE
├─ CELLULE 8 ──── nettoyage géographique (adresse, NUTS2, région, NUTS3)
├─ CELLULE 9 ──── chargement des référentiels, zones pays, doublons,
│                 enrichissement GLOBAL + Référence projet complétée
├─ CELLULE 10 ─── Étapes C, D, E, SGAE, NAF
├─ CELLULE 11 ─── export du fichier + règle AUTRE / A CHERCHER
├─ CELLULE 13 ─── chargement des référentiels et du fichier FTS
└─ CELLULE 15 ─── ORCHESTRATION (à lancer en dernier)
```

### 4.2 Les référentiels embarqués

Volumes constatés dans la **v5_41** (compteurs affichés par la cellule 13) :

| Référentiel | Volume | Contenu | Source |
|---|---|---|---|
| `GEO_FRANCE` | 108 départements | département → NUTS3, région, NUTS2, territoire ; désambiguïsation par CP et par pays | table SGAE + Eurostat NUTS 2021 |
| `VILLES_FR` | 31 950 | commune → département | COG INSEE |
| `FORMES_JURIDIQUES` | 269 codes | catégories juridiques, 3 niveaux | INSEE `cj_septembre_2022` |
| `REF_SGAE` | 262 | code juridique → catégorie simplifiée | référentiel métier SGAE |
| `NAF_REV2` | 732 | code NAF → libellé | INSEE NAF rév. 2 |
| `NAF2025_SECTIONS` | 22 sections / 87 divisions | division → section NAF 2025 | INSEE NAF 2025 |
| `OPERATEURS_ETAT` | **443 SIREN**, 440 programmes, **438 périodes de validité** | SIREN → opérateur, programme chef de file, années de validité | annexe « Jaune » opérateurs de l'État |
| `SOUS_CATEGORIES` | **198 programmes** | programme → famille | Commission / SGAE |
| `ETATS` | 32 | pays → code ISO, zone | Commission / SGAE |
| `PAYS_ALIAS` | 27 alias ISO, 27 territoires FR | variantes de libellés pays | ISO + observation du FTS |
| `CFP_REGLES` | 10 instruments hors CFP | périodes CFP et exceptions | Commission |

### 4.3 Le déroulé complet (cellule 15)

```
  fichier FTS brut (df_brut)
        │
        ├─ ÉTAPE 0 ── enrichissement GLOBAL (tous pays)
        │             zone UE/AELE/UK · période CFP · dépense CFP
        │             sous-catégorie · nom nettoyé · n° et type de projet
        │             Référence projet complétée              ← v5_41
        │             └──> export <nom>_GLOBAL.xlsx
        │
        ├─ FILTRE ─── lignes France uniquement
        │
        ├─ ÉTAPE A ── PASSE 0 : SIREN fourni par la Commission (Main Registration)
        │             PASSE MÉMOIRE : SIREN connu d'un autre millésime
        │             PASSE 1 : lignes restantes sans TVA → RECHERCHE (API)
        │             PASSE 2 : lignes avec TVA → DOCUMENTATION
        │
        ├─ ÉTAPE A2 ─ réconciliation des doublons de graphie
        ├─ ÉTAPE A3 ─ complétion géographique par le siège SIRENE
        ├─ ÉTAPE E ── correction des états « Cessée » faussés
        ├─ NETTOYAGE  adresse réordonnée · CP corrigé · NUTS2 · Région · NUTS3
        ├─ ÉTAPE C ── Metro / RUP / PTOM
        ├─ ÉTAPE D ── opérateur de l'État + programme (avec période)
        ├─ ÉTAPE SGAE statut juridique simplifié
        ├─ ÉTAPE NAF  libellé d'activité principale + section NAF 2025
        │
        ├─ NOM API ── écriture officielle de l'annuaire
        ├─ FINITION ─ « AUTRE » / « A CHERCHER » (généralisée)
        │             retrait des colonnes de travail
        │
        └──> <nom>_FRANCE.xlsx + <nom>_RAPPORT_TVA.xlsx + correspondance_siren.json
```

---

## 5. Le traitement GLOBAL (Étape 0) — toutes lignes, tous pays

Avant tout traitement français, le pipeline enrichit **l'intégralité** du fichier,
quelle que soit la nationalité du bénéficiaire. Cette étape produit le fichier
`<nom>_GLOBAL.xlsx` et **neuf colonnes**. Elle ne fait **aucun appel API** :
tout est calculé à partir des référentiels embarqués — elle est donc rapide.

### 5.1 Les colonnes produites

| Colonne | Position | Contenu |
|---|---|---|
| `Bénéficiaire corrigé` | après le nom | nom débarrassé des étoiles finales |
| `FR/UE/UK/AELE/AUTRE` | après le pays | zone géopolitique |
| `Etats` | après la zone | code ISO du pays |
| `Sous catégorie` | **avant** `Programme name` | famille de programme |
| `Période CFP` | après `Programme name` | cadre financier pluriannuel |
| `Dépense CFP` | après `Période CFP` | période d'**imputation** réelle |
| **`Référence projet complétée`** | après `Project / Contract Reference` | référence numérique du projet — **v5_41** |
| `N° projet` | après la référence LC | numéro séquentiel de projet |
| `Type de projet` | après `N° projet` | **Mono** / **Collaboratif** / **Indéterminé** |

### 5.2 `Bénéficiaire corrigé` — un nettoyage volontairement minimal

Seules les **étoiles finales** sont retirées : `« NOM* »` → `« NOM »`.

> **Pourquoi si peu ?** Les étoiles *internes* (`« NOM*SIGLE »`) séparent souvent
> deux désignations réellement distinctes — parfois même deux entités liées mais
> différentes. Les supprimer détruirait de l'information exploitée ensuite par
> les stratégies `STAR` et `SIGLE` du moteur. Les noms entièrement composés
> d'étoiles (bénéficiaires anonymisés) sont laissés **intacts**.

Depuis la v5_39, cette colonne reçoit également le **nom officiel SIRENE** pour
les lignes identifiées par la Passe 0 ou la Passe mémoire.

### 5.3 `FR / UE / UK / AELE / AUTRE` et `Etats`

Classement en cinq zones, avec trois particularités :

1. Les **DOM-TOM et RUP** sont reconnus comme **FR**, même écrits sous leur nom
   propre (Guadeloupe, Nouvelle-Calédonie…).
2. Un dictionnaire d'**alias** absorbe les variantes de libellé (traductions,
   graphies anciennes, troncatures de l'export Commission — `« Republ »` → Serbie,
   `« Republic o »` → Monténégro).
3. Si le pays est inconnu du référentiel, un **repli ISO** tente de retrouver le
   code ; à défaut, la ligne est classée `AUTRE` et **signalée** en fin
   d'exécution avec son nombre d'occurrences.

### 5.4 `Période CFP` — le cadre financier de l'année

| Année | Période |
|---|---|
| 2007-2013 | `07-13` |
| 2014-2020 | `14-20` |
| 2021-2027 | `21-27` |
| hors plage | vide, **signalé** en fin d'exécution |

Exception : les **instruments hors CFP** (Fonds européen de développement, et
la liste `HORS_CFP_PROGRAMMES`) reçoivent `Hors CFP`, car ils ne relèvent pas du
budget général de l'Union.

### 5.5 `Dépense CFP` — la distinction la plus subtile du pipeline

> **Ne pas confondre `Période CFP` et `Dépense CFP`.**
> `Période CFP` répond à « en quelle année ce paiement a-t-il eu lieu ? ».
> `Dépense CFP` répond à « à quelle programmation cette dépense
> **appartient-elle** ? ».

Un paiement effectué en 2022 peut solder un programme 2014-2020 : sa
`Période CFP` est `21-27` mais sa `Dépense CFP` est `2014-2020`. Le rattachement
se fait en analysant le libellé de la ligne budgétaire (`Budget line name`) :

| Indice dans le libellé | Interprétation |
|---|---|
| `prior to 2007`, `before 2007` | Avant 2007 |
| `2007 to 2013` | 2007-2013 |
| `prior to 2014` | programmation précédente |
| `completion of…`, `former…`, `previous…` | **achèvement** d'une programmation antérieure |
| aucun indice | la période courante |

> **Cas notable** : sur les lignes 2021-2027, la seule mention `completion of…`
> suffit à rattacher la dépense à 2014-2020, même sans le mot `previous` — règle
> établie après examen de 23 cas sur 24 du référentiel.

### 5.6 `Sous catégorie`

Rattache chaque `Programme name` à une famille de programmes (**198 programmes**
référencés depuis la v5_33). Un programme inconnu reçoit `NA` et est **listé
nommément** en fin d'exécution, avec son volume : c'est le signal qu'il faut
enrichir `SOUS_CATEGORIES`.

### 5.7 `Référence projet complétée` — nouveauté v5_41

Les trois colonnes d'identification de projet de la Commission
(`Call for proposals Reference`, `Project / Contract Reference`,
`Project / Contract Acronym`) ne sont alimentées **qu'à partir du millésime
2025**. Sur tous les millésimes antérieurs, elles portent
`N/A - Not applicable` — alors que la référence figure bel et bien **en tête du
libellé** `Subject of grant or contract` :

```
« 101177660 - MAKE-A-THEK - MODULAR LIBRARY MAKERSPACES FOR HERITAGE CRAFTS… »
   └ référence du projet
```

Une colonne `Référence projet complétée` est donc créée **juste après**
`Project / Contract Reference`, sans jamais modifier celle-ci (règle 3) :

| Cas | Contenu de la colonne |
|---|---|
| la Commission a renseigné la référence | sa valeur, **recopiée telle quelle** |
| colonne d'origine vide ou `N/A` | la référence extraite du libellé |
| aucune référence identifiable | **`NA`** |

**La référence est toujours NUMÉRIQUE**, de 6 à 9 chiffres. Deux motifs
seulement sont reconnus :

| Motif | Exemple | Résultat |
|---|---|---|
| `NUM_EN_TETE` | `643271 - ERACoSysMed - …` | `643271` |
| `NUM_APRES_CODE_APPEL` | `KBBE-2013 613960 SMARTBEES` | `613960` |

Dans le second cas, le code de tête (`KBBE-2013`, `FP7-SEC-2013`…) désigne
l'**appel à propositions**, pas le projet : seul le nombre est retenu, et
uniquement s'il apparaît dans les 60 premiers caractères du libellé.

> **Tout ce qui n'est pas numérique est refusé.** Les codes TEN-T
> (`2012-DE-17022-S`), les codes à barres obliques (`VS/2014/0582`) et les codes
> à tirets (`TA-1117`) ne sont **pas** capturés : ce sont des identifiants
> d'une autre nature. De même, le plancher de 6 chiffres écarte les nombres
> courts en tête de libellé, qui sont des années ou des numéros d'ordre
> (`2013 11 02 OPTTEST…`, `2014 86 01 NEGOTIATED…`).

Le bilan de fin d'étape distingue les trois cas (reprises de la Commission,
dérivées, `NA`), donne la répartition par motif et liste les 15 premières formes
de libellés non reconnues, pour arbitrage.

> **Contrôle recommandé au premier passage** : sur un fichier **2025**, où la
> Commission renseigne déjà tout, comparez sa référence et celle qu'aurait
> produite la dérivation. C'est le test de fiabilité le plus direct avant
> d'appliquer la règle aux millésimes antérieurs.

### 5.8 `N° projet` et `Type de projet` — l'analyse collaborative

C'est l'enrichissement le plus élaboré de l'étape globale.

**Définition** : un **projet** = un engagement juridique, identifié par la
`Reference of the Legal Commitment (LC)`. Plusieurs lignes partageant la même
référence LC constituent **un seul projet**, éventuellement porté par plusieurs
bénéficiaires.

**Classification** :

| Bénéficiaires distincts sur le projet | Type |
|---|---|
| 1 | **Mono** |
| ≥ 2 | **Collaboratif** |
| référence LC absente | **Indéterminé** |

**Le problème à résoudre** : compter les bénéficiaires *distincts* suppose de
savoir que « UNIVERSITÉ DE X » et « UNIVERSITY OF X » sont la même entité.
Compter naïvement les libellés surestimerait la collaboration.

**La solution — fusion transitive (*union-find*)**. Deux lignes du même projet
sont réputées désigner le **même bénéficiaire** si elles partagent au moins un
de ces trois identifiants :

| Clé | Force | Remarque |
|---|---|---|
| **Nom de base** | moyenne | étoile et suffixes juridiques (`SA`, `GMBH`, `LTD`, `BV`…) retirés |
| **Numéro de TVA** | **forte** | identifiant le plus fiable |
| **Adresse complète** | forte | pays + rue + CP + ville ; ignorée si trop courte ou anonymisée |

La fusion est **transitive** : si A et B partagent une TVA, et B et C une
adresse, alors A, B et C ne comptent que pour **un** bénéficiaire. C'est ce qui
permet de rapprocher des libellés rédigés en plusieurs langues sur un même
projet européen.

### 5.9 Les alertes de fin d'étape

L'étape globale se termine par un bilan à lire attentivement :

```
Zone géographique : FR 12 345 | UE 6 789 | UK 234 | AELE 56 | AUTRE 78
Période CFP : 14-20 5 000 | 21-27 14 000 | Hors CFP 500
Dépense CFP : 2014-2020 5 500 | 2021-2027 13 500 | Hors CFP 500
Référence projet complétée : Commission 5 813 | dérivées 12 004 | NA 2 690
Projets : 8 234 distincts | lignes Mono 15 000 | Collaboratif 4 000 | Indéterminé 500
⚠️ 3 pays sans code Etat (absents d'ETATS + ISO introuvable) :
⚠️ 12 programme(s) absents de SOUS_CATEGORIES (Sous catégorie = NA) :
```

Chaque `⚠️` désigne un référentiel à compléter, avec le volume concerné pour
arbitrer la priorité. Ces alertes ne bloquent pas le traitement.

---

## 6. Le moteur de recherche : chaque stratégie en détail

### 6.1 Principe

Le moteur interroge l'API **Recherche d'entreprises**, adossée à **SIRENE**.
Pour un bénéficiaire donné :

```
   nom brut du FTS
        │
   ① _detecter_type()      → COMMUNE / ASSOCIATION / AUTRE
        │
   ② _generer_requetes()   → liste ORDONNÉE de requêtes (voir §6.3)
        │
   ③ pour chaque requête, dans l'ordre :
        _appeler_api()  →  jusqu'à 8 candidats
             │
        _scorer()       →  note 0-100 par candidat
             │
        meilleur ≥ SEUIL_SCORE (80) ?  ── OUI ──► on s'arrête, on retient
             │ NON
        requête suivante
        │
   ④ aucune requête n'aboutit → NON_TROUVE
```

**L'ordre des requêtes est capital** : de la plus précise à la plus large. La
première qui aboutit gagne, les suivantes ne sont pas tentées — c'est à la fois
une garantie de qualité (le résultat le plus contraint l'emporte) et une
économie d'appels API.

> **Le moteur n'est sollicité qu'en Passe 1**, c'est-à-dire pour les lignes qui
> n'ont ni TVA, ni Main Registration exploitable, ni correspondance en mémoire.

### 6.2 La détection de type

`_detecter_type()` examine le **début** du nom :

| Type | Déclencheurs (en début de nom) |
|---|---|
| `COMMUNE` | `COMMUNE`, `VILLE DE`, `MAIRIE`, `COMMUNAUTE` |
| `ASSOCIATION` | `ASSOCIATION`, `ASSOC.`, `FEDERATION`, `LIGUE`, `AMICALE`, `COMITE`, `UNION`, `COLLECTIF`, `GROUPEMENT`, `MOUVEMENT` |
| `AUTRE` | tout le reste (entreprises, établissements publics, universités…) |

Le type détermine **quelles stratégies** seront générées et dans quel ordre.

### 6.3 Catalogue complet des stratégies

Chaque requête porte un **code** qui apparaît dans la colonne `Strategie` du
rapport : c'est la trace de la façon dont le bénéficiaire a été retrouvé.

#### Stratégies « COMMUNE » (préfixe `C`)

| Code | Requête envoyée | Filtre CP | Intention |
|---|---|---|---|
| `C1_NOM_ADR_CP` | nom (sans parenthèses) + adresse | oui | la plus contrainte |
| `C2_NOM_CP` | nom seul | oui | nom + département |
| `C3_NOM` | nom seul | non | si le CP du FTS est faux |
| `C4_NOM_BRUT_CP` | nom **avec** parenthèses | oui | si les parenthèses portaient du sens |

> **Garde-fou collectivité.** Pour ce type, le résultat n'est accepté que si le
> nom renvoyé par l'API **commence** par un marqueur de collectivité (`COMMUNE`,
> `MAIRIE`, `VILLE DE`, `COMMUNAUTE`, `METROPOLE`, `DEPARTEMENT`, `REGION`,
> `SYNDICAT`, `SIVOM`, `SIVU`, `SDIS`, `PETR`…). Sans lui, une requête
> « commune de X » remonterait le club de football, le comité des fêtes ou la
> caisse des écoles de la même ville — faux positifs classiques.
>
> Le nom complet `« COMMUNE DE X »` est **toujours conservé** : on n'extrait
> jamais le seul nom de la ville (consigne métier).

#### Stratégies « ASSOCIATION » (préfixe `A`)

| Code | Requête envoyée | Filtre CP | Intention |
|---|---|---|---|
| `A1_SANS_ENTETE_ADR_CP` | nom sans mot d'en-tête + adresse | oui | la plus contrainte |
| `A2_SANS_ENTETE_CP` | nom sans mot d'en-tête | oui | — |
| `A3_SANS_ENTETE` | nom sans mot d'en-tête | non | — |
| `A4_NOM_ADR_CP` | nom complet + adresse | oui | — |
| `A5_NOM_CP` | nom complet | oui | — |
| `A6_NOM` | nom complet | non | — |
| `A7_MOTS_CLES` | 4 premiers mots significatifs | oui | noms très longs |
| `A8_NOM_VILLE` | 2 premiers mots + ville | non | quand le CP échoue |
| `A9_SANS_GEO` | nom sans qualificatif géographique | non | « … DE BRETAGNE » retiré |

> **La troncature conditionnelle de l'en-tête** est une subtilité importante.
> Le mot d'en-tête (`ASSOCIATION`, `FEDERATION`, `COMITE`…) n'est retiré **que**
> s'il est suivi **directement d'un mot plein** :
>
> | Nom FTS | Traitement | Raison |
> |---|---|---|
> | `ASSOCIATION DUPONT` | → `DUPONT` | suivi d'un mot plein |
> | `ASSOCIATION DE LA JEUNESSE` | **conservé** | suivi de `DE` |
> | `COMITE POUR LE SPORT` | **conservé** | suivi de `POUR` |
>
> Les prépositions bloquantes sont `DE, DU, DES, D', LA, LE, LES, L', POUR, A,
> AU, AUX, EN, ET, SUR, AVEC`. La logique : si le mot d'en-tête est suivi d'une
> préposition, il fait **partie intégrante** du nom officiel.

#### Stratégies « AUTRE » (préfixe `E`)

| Code | Requête envoyée | Filtre CP | Intention |
|---|---|---|---|
| `E1_NOM_ADR_CP` | nom + adresse | oui | la plus contrainte |
| `E3_NOM_CP` | nom seul | oui | — |
| `E4_NOM` | nom seul | non | — |
| `E6_SANS_SUFFIXE` | nom sans forme juridique (`SA`, `SAS`, `SARL`, `GMBH`…) | oui | — |
| `E7_MOTS_CLES` | 4 premiers mots significatifs | oui | noms très longs |
| `E8_NOM_NORMALISE` | nom sans accents ni ponctuation | oui | caractères parasites |
| `E9_NOM_TRONQUE` | 50 premiers caractères (coupés sur un mot) | non | noms > 55 caractères |
| `E10_SANS_GEO` | nom sans qualificatif géographique | non | — |

> **`E2_CORE_ADR_CP` a été supprimée** : cette stratégie découpait le nom en
> « noyau » et générait des faux positifs. Sa suppression fait partie du
> calibrage figé — ne pas la réintroduire.

#### Stratégies transverses (tous types)

| Code | Requête envoyée | Filtre CP | Intention |
|---|---|---|---|
| `P1_SANS_PAR_CP` | nom sans les `(...)` | oui | retire `(ISERE)`, `(SIOFA)`… |
| `P2_SANS_PAR` | nom sans les `(...)` | non | — |
| `SIGLE` | le segment **le plus court** entre étoiles (≥ 4 caractères) | oui | `NOM COMPLET*ADEME` → `ADEME` |
| `STAR_APRES_CP` | segment **complet** après `*` (> 6 caractères) | oui | vise une structure rattachée |
| `STAR_APRES` | idem | non | — |

> **Les stratégies `STAR` échappent au garde-fou collectivité** : le segment
> après l'étoile désigne souvent une **autre entité** (comité, école, structure
> rattachée) qui n'est légitimement pas une collectivité.

#### Stratégies génériques de repli (préfixe `G`)

Tentées en dernier, quel que soit le type détecté :

| Code | Requête envoyée | Filtre CP |
|---|---|---|
| `G1_NOM_COMPLET_CP` | nom brut intégral | oui |
| `G2_NOM_COMPLET` | nom brut intégral | non |
| `G3_NOM_PRINCIPAL` | segment **le plus long** entre étoiles | oui |
| `G5_SANS_SUFFIXE` | nom sans forme juridique | oui |
| `G6_MOTS_CLES` | 4 premiers mots significatifs | oui |
| `G8_NOM_TRONQUE` | 42 premiers caractères | non |
| `G9_NOM_VILLE` | 2 premiers mots + ville | non |

#### Stratégies hors moteur (enveloppe et passes amont)

Elles apparaissent aussi dans la colonne `Strategie` du rapport :

| Code | Signification |
|---|---|
| `MAIN_REGISTRATION` | **Passe 0** : SIREN fourni par la Commission, validé par la clé de Luhn (score 100) |
| `MEMOIRE_CORRESPONDANCE` | **Passe mémoire** : couple (nom, département) déjà résolu lors d'un autre millésime (score 100) |
| `MEMOIRE_NOM_SEUL` | Passe mémoire, repli : le nom ne correspond qu'à **un seul** SIREN dans toute la mémoire (déménagement entre millésimes) |
| `ALIAS_SIREN` | SIREN imposé par une correction vérifiée ou un alias (score forcé à 100) |
| `FAUX_POSITIF_ECARTE` | résultat annulé : couple (bénéficiaire, SIREN) banni |
| `SANS_TVA_MANUEL` | bénéficiaire exclu de toute recherche (République française) |
| `TVA_EXISTANTE` | passe 2 : la TVA venait du FTS, on n'a fait que documenter |
| `REPLI_EXACT` | **repli de dernière chance** : aucune requête n'a atteint le seuil de 80, mais le meilleur candidat rencontré porte un nom **exact à ≥ 90 %** (`SEUIL_EXACTITUDE_REPLI`). Il est alors accepté. Ce repli récupère des identifications que le score global rejetait à cause de divergences d'adresse, sans jamais assouplir le seuil principal. |

### 6.4 Outils de transformation des noms

| Fonction | Rôle | Exemple |
|---|---|---|
| `_normaliser` | majuscules, accents retirés, ponctuation → espaces | `Café-Théâtre` → `CAFE THEATRE` |
| `_supprimer_suffixes` | retire les formes juridiques | `DUPONT SAS` → `DUPONT` |
| `_mots_cles(n)` | garde les `n` premiers mots significatifs | filtre `DE, DU, LA, THE, OF…` |
| `_supprimer_geo` | retire les qualificatifs géographiques | `… DE NORMANDIE` → `…` |
| `_tronquer_entete` | retire l'en-tête associatif **si** suivi d'un mot plein | voir §6.3 |
| `_sigle` | isole le segment court entre étoiles | `X*ADEME` → `ADEME` |
| `_dedup` | supprime les requêtes en doublon | évite les appels inutiles |

### 6.5 Le scoring, signal par signal

`_scorer()` construit la note en six temps, et **journalise chaque
contribution** (colonne d'audit du rapport) :

**① Similarité textuelle** — trois mesures `rapidfuzz` :

| Mesure | Ce qu'elle tolère |
|---|---|
| `token_sort_ratio` | l'**ordre** des mots |
| `token_set_ratio` | les mots **en trop** |
| `partial_ratio` | l'**inclusion** d'un nom dans l'autre |

**② Pondération par la longueur** — si les deux noms ont des longueurs
comparables (ratio ≥ 0,60), `partial_ratio` est pris en compte (pondéré à 88 %).
Sinon il est **ignoré** et le score subit −10 %.

> *Pourquoi ?* Sans ce garde-fou, un nom très court inclus dans un nom très long
> obtiendrait 100 % en `partial_ratio` — « SNCF » matcherait n'importe quelle
> raison sociale contenant ces lettres.

**③ Adresse** — bonus si l'adresse concorde, malus de 5 % sinon (uniquement si
le score est déjà < 95).

**④ Ville** — bonus **+5** si la similarité ≥ 80 ; malus 5 % si < 35.

**⑤ Département** — bonus **+3** si les deux premiers chiffres du CP
concordent ; malus 3 % sinon (uniquement si score < 93).

**⑥ État administratif** — pénalité **désactivée** (`PENALISER_RADIEES = False`).

> **La règle du « nom parfait ».** Si le nom correspond **exactement** à celui de
> l'API (exactitude 100, mots juridiques neutralisés), **toutes les pénalités
> géographiques sont désactivées** — les bonus restent actifs.
>
> *Justification* : une identité de nom certaine ne doit pas être dégradée parce
> que le FTS porte l'adresse d'une antenne et SIRENE celle du siège. C'est un
> cas de figure très fréquent pour les grandes structures.

---

## 7. Les règles intangibles

Ces règles ne sont pas des préférences de style : chacune corrige un incident
constaté. Les enfreindre a un coût mesuré en TVA perdues.

### Règle 1 — Ne jamais modifier le moteur

Les cinq fonctions `rechercher_tva`, `_scorer`, `_generer_requetes`,
`_appeler_api`, `_est_exclu` et les constantes `SEUIL_SCORE = 80`,
`SEUIL_HAUTE_CONF = 92`, `PENALISER_RADIEES = False` sont **figées**.

*Pourquoi* : le moteur résulte d'un calibrage empirique par comparaison de
versions. Chaque retouche antérieure a fait perdre des TVA correctes.

*Comment évoluer malgré tout* : par l'**enveloppe** `rechercher_tva_plus`, ou en
**lecture seule** sur la réponse de l'API. Toutes les fonctionnalités ajoutées
depuis la v5_2 l'ont été ainsi, sans toucher une ligne du moteur — y compris les
Passes 0 et mémoire, qui s'intercalent **avant** lui, et la colonne `Nom API`,
obtenue par une enveloppe de lecture (`_fiche_avec_nom`).

*Vérification obligatoire à chaque livraison* : comparaison bit à bit du corps
des cinq fonctions avec la version de référence (§13).

### Règle 2 — Chercher sur les données brutes

La recherche utilise le nom, l'adresse et le code postal **d'origine**. Le
nettoyage sert uniquement à l'affichage (`Bénéficiaire corrigé`,
`Adresse réordonnée`).

*Pourquoi* : nettoyer avant de chercher supprime des sigles discriminants et
dégrade le taux d'identification.

### Règle 3 — Ne jamais modifier une colonne d'origine

Toute information calculée va dans une **colonne nouvelle**, placée juste après
sa colonne source, peinte en **bleu `BDD7EE`**.

*Pourquoi* : le fichier livré doit rester auditable ; on doit toujours pouvoir
comparer la valeur calculée à la valeur d'origine.

*Application récente* : la v5_40 a créé `Adresse réordonnée` plutôt que de
corriger la colonne `Address` de la Commission ; la v5_41 a créé
`Référence projet complétée` plutôt que de remplir
`Project / Contract Reference`.

### Règle 4 — Ne pas pénaliser les entités radiées

`PENALISER_RADIEES = False`. L'état radié n'influence **jamais** le score.

*Pourquoi* : de nombreuses universités et EPA ont un SIREN historique cessé qui
coexiste avec l'actuel. Pénaliser ferait perdre des appariements corrects.
L'état erroné est corrigé **après coup** par l'Étape E.

### Règle 5 — Sources officielles uniquement

Toute table de référence provient d'une source officielle (INSEE, INPI,
Eurostat, data.gouv.fr, portails ministériels) ou de l'utilisateur, et est
**vérifiée avant intégration** : clé de Luhn pour tout SIREN/SIRET, recoupement
web en cas de doute.

*Incident fondateur* : une table d'opérateurs comportait **30 SIREN erronés**,
détectés en comparant deux versions du fichier source et confirmés par
recoupement officiel. Ne jamais présumer qu'un fichier « nettoyé » est correct.

### Règle 6 — Affichage en liste

Progression affichée ligne par ligne :
`[n/total  xx.x%] nom → ✅ … / ❌ … (cause)`, jamais de barre de progression.

*Pourquoi* : sur un traitement de plusieurs heures, la trace textuelle permet de
diagnostiquer *a posteriori* ce qui a échoué et pourquoi.

### Règle 7 — Appels API robustes

Réessais à pause croissante (3-4 tentatives), `DELAI_API` entre appels, et
**caches globaux** (`CACHE_FICHES_SIREN`, `_CACHE_SUCCESSEUR_ACTIF`) : un même
SIREN n'est jamais interrogé deux fois dans une session.

### Règle 8 — Vérifier les faits avant de conclure à un manque

Avant d'affirmer qu'un référentiel est incomplet, **vérifier le compte officiel**.

*Incident fondateur* : trois familles d'opérateurs (ARS, agences de l'eau, parcs
nationaux) ont été signalées comme incomplètes. Vérification faite : **18/18**
ARS, **6/6** agences de l'eau, **11/11** parcs nationaux figuraient déjà au
référentiel. Les lignes `« … GLOBAL »` sont des **agrégats de famille**, pas des
entités : elles n'ont pas de SIREN propre et ne doivent jamais être recherchées.
---

## 8. Les enrichissements, étape par étape

### Étape 0 — Enrichissement global

Toutes lignes, tous pays : zone (`FR` / `UE` / `UK` / `AELE` / `AUTRE`), code
ISO, période CFP, dépense CFP, sous-catégorie de programme, nom nettoyé, numéro
et type de projet, et **référence projet complétée** (§5.7). Produit le fichier
`_GLOBAL`.

### Étape A — Enrichissement TVA (quatre passes)

L'ordre est une **cascade du plus certain au plus coûteux** : chaque passe ne
traite que ce que les précédentes n'ont pas résolu.

| | Passe 0 | Passe mémoire | Passe 1 | Passe 2 |
|---|---|---|---|---|
| **Cible** | lignes sans TVA mais avec *Main Registration* | lignes sans TVA dont le couple (nom, département) est déjà connu | lignes restantes sans TVA | lignes **avec** TVA |
| **Action** | lecture directe du SIREN | réutilisation du SIREN mémorisé | recherche via le moteur | documentation seulement |
| **Appels de recherche** | **aucun** | **aucun** | plusieurs par ligne | aucun (une fiche par SIREN, mise en cache) |
| **Score / stratégie** | 100 / `MAIN_REGISTRATION` | 100 / `MEMOIRE_*` | variable | 100 / `TVA_EXISTANTE` |

**Passe 0 — le SIREN fourni par la Commission.** L'export FTS 2025 introduit la
colonne `Main registration number of beneficiary`. Sur les lignes françaises
vérifiées, 5 466 sur 5 813 portaient un SIREN, **100 % valides au sens de la clé
de Luhn**, recoupés indépendamment sur des cas connus (CNRS `180089013`,
Airbus `383474814`, Thales `552059024`).

> **C'est un champ générique européen** : chaque pays y inscrit son registre
> national dans son propre format (Allemagne `VR7795`, Belgique numéro
> d'entreprise, Italie et Espagne codes provinciaux). Il n'est donc exploité que
> pour les **lignes France**, et seulement si la valeur ressemble à un SIREN
> (9 chiffres) **et** passe la clé de Luhn. Un SIRET fourni par erreur (14
> chiffres) voit ses 9 premiers chiffres repris. Colonne absente (millésimes
> antérieurs à 2025) : la passe est ignorée proprement.
>
> Les bénéficiaires « sans TVA possible » (République française) n'entrent
> **jamais** dans cette passe.

**Passe mémoire — réutiliser un millésime sur l'autre.** Un export FTS ne
contient qu'un seul millésime. Les SIREN certains obtenus par la Passe 0 ou par
la Passe 2 sont donc **mémorisés** sous la clé « nom normalisé + département
déduit du code postal », puis exportés dans `correspondance_siren.json`. Au
traitement d'un millésime ancien (2014-2024, sans *Main Registration*), il
suffit de redéposer ce fichier : les bénéficiaires déjà connus sont documentés
directement, **sans aucun appel de recherche**.

> **Pourquoi la clé comporte le département** et non le nom seul : pour ne pas
> confondre deux implantations réellement distinctes portant le même nom (une
> chambre de commerce régionale, par exemple).
>
> **Repli « nom seul »** (v5_36) : si le couple (nom, département) ne
> correspond pas mais que le nom ne renvoie qu'à **un seul** SIREN dans toute la
> mémoire, celui-ci est réutilisé — cas des déménagements entre millésimes
> (CARE FRANCE, 93 → 75). Plusieurs SIREN pour un même nom : on ne tranche pas,
> la recherche normale reprend. L'étiquette `MEMOIRE_NOM_SEUL` rend le cas
> repérable au rapport.
>
> **Socle embarqué** (v5_37) : 338 correspondances fiables du millésime 2025
> sont intégrées **en dur** dans le notebook. Le dépôt du fichier JSON n'est
> donc pas obligatoire ; s'il est fourni, il se fusionne par-dessus le socle et
> ses entrées l'emportent.
>
> **Portée intra-fichier** (v5_34) : la mémoire est alimentée dès la lecture du
> *Main Registration*, avant le calcul du périmètre de recherche. Un
> bénéficiaire présent sur plusieurs lignes du même fichier, dont une seule
> porte un *Main Registration*, en fait donc profiter ses lignes sœurs.

**Passe 1 — la recherche.** Elle ne porte plus que sur les lignes qui n'ont ni
TVA, ni *Main Registration* exploitable, ni correspondance en mémoire. C'est la
seule passe qui sollicite le moteur figé (§6).

**Passe 2 — la documentation.** Une TVA fournie par la Commission n'est
**jamais** remise en cause, seulement enrichie : SIRET, forme juridique, NAF,
état, et depuis la v5_40, `Nom API`.

### Étape A2 — Réconciliation des doublons

Un bénéficiaire peut apparaître sous plusieurs graphies. Si l'une a trouvé une
TVA et l'autre non, la seconde est **alignée** sur la première. Le rapprochement
se fait sur une clé normalisée, jamais sur une similarité approximative.

### Étape A3 — Complétion géographique par SIRENE

Pour les lignes France dont l'adresse est vide ou masquée mais dont le SIREN est
connu : lecture de l'adresse du **siège** dans SIRENE, versée dans trois
colonnes de **travail** (`Adresse (SIRENE)`, `Ville (SIRENE)`,
`Code postal (SIRENE)`).

> Ces colonnes alimentent les calculs en aval puis sont **retirées du fichier
> final**. Ce sont des échafaudages, pas des livrables.

### Désambiguïsation d'établissement (best-effort)

Pour les bénéficiaires multi-établissements identifiés par la Passe 0 ou la
Passe mémoire : quand le code postal de la ligne FTS diffère de celui du siège,
une recherche par nom + département tente de retrouver l'établissement
correspondant à **ce** code postal, dont le SIRET remplace celui du siège.

> **Limite assumée et vérifiée.** L'API publique
> `recherche-entreprises.api.gouv.fr` ne permet **pas** de lister tous les
> établissements d'un SIREN : un filtre géographique combiné à une requête SIREN
> est silencieusement ignoré, et `matching_etablissements` reste souvent vide.
> Le contournement implémenté est légitime mais **best-effort** : en l'absence
> de résultat probant, repli systématique sur le SIRET du siège. Jamais
> d'établissement deviné. Une désambiguïsation garantie supposerait l'API Sirene
> authentifiée de l'INSEE, hors du périmètre actuel.

### Étape E — Correction des états « Cessée » faussés

*Problème* : une université fusionnée conserve un ancien SIREN cessé portant le
même nom que le nouveau. Le moteur (qui ne pénalise pas les radiées) retient
parfois l'ancien : le fichier affiche « Cessée » pour un établissement en
activité, ce qui fausserait toute analyse.

*Correctif* : pour chaque ligne « Cessée », recherche d'un successeur **actif**
(`etat_administratif=A`) de nom **exactement identique**, dans le **même
département**. Deux garde-fous : correspondance exacte, et candidat unique. Une
entité réellement fermée reste « Cessée ».

*Exemple* : Université Clermont Auvergne — `130022775` (cessé) → `130028061`
(actif).

### Nettoyage de l'adresse

Les adresses FTS sont fréquemment rédigées « voie puis numéro »
(`RUE BISCORNET 9`). Une colonne **`Adresse réordonnée`** est créée juste après
l'adresse d'origine : elle porte l'adresse remise à la norme française
(`9 RUE BISCORNET`) quand une correction s'applique, **sinon l'adresse d'origine
telle quelle**. Les cellules réellement réordonnées sont surlignées.

> La colonne `Address` de la Commission n'est **plus jamais modifiée**
> (règle 3). Le moteur de recherche continue d'utiliser l'adresse **brute**
> (règle 2) — et il s'exécute de toute façon **avant** le réordonnancement :
> aucun impact sur les TVA trouvées.

### Nettoyage géographique

Chaîne de repli, du plus fiable au moins fiable :

```
1. code postal BRUT du FTS
2. code postal du SIÈGE SIRENE          (étape A3)
3. pays                                  (collectivités d'outre-mer)
4. NUTS2 brut déjà présent
5. VILLE via le référentiel des communes (dernier recours)
```

Dès qu'un maillon aboutit, on s'arrête. Si aucun n'aboutit, la case reste
**vide** : on n'invente jamais une région.

> **Piège des graphies.** Les référentiels divergeaient (« Grand-Est » vs
> « Grand Est »). La table départements **fait foi** (avec tiret) et la table
> NUTS2→région est harmonisée sur elle au chargement. Toute comparaison passe
> par une clé sans tiret, espace ni accent.

> **Homonymes.** Les communes ambiguës (SAINT-DENIS : métropole *et* La Réunion)
> sont volontairement absentes du référentiel : mieux vaut ne rien conclure que
> se tromper de territoire.

### Étape C — Metro / RUP / PTOM

Distinction **juridique** européenne, pas géographique :

| Statut | Base juridique | Territoires | Éligibilité |
|---|---|---|---|
| **Metro** | — | France métropolitaine | droit commun |
| **RUP** | art. 349 TFUE | Guadeloupe, Martinique, Guyane, La Réunion, Mayotte, Saint-Martin | **dans** l'UE, éligibles aux fonds structurels |
| **PTOM** | annexe II TFUE | Nouvelle-Calédonie, Polynésie française, Saint-Barthélemy, Wallis-et-Futuna, Saint-Pierre-et-Miquelon, TAAF | **hors** UE, régime d'association |

> **Piège classique** : Saint-Barthélemy (`97133`) est **PTOM**, Saint-Martin
> (`97150`) est **RUP**. Deux îles voisines, deux régimes opposés, distingués
> au code postal.

### Étape D — Opérateurs de l'État

Le référentiel couvre désormais **443 SIREN** (annexe « Jaune » complète), avec
**période de validité** pour 438 d'entre eux. Trois mécanismes :

1. **Par SIREN** — appartenance au référentiel ; `Programme_Operateur` reçoit le
   programme budgétaire chef de file.
2. **Par période** — `Operateur_Etat` vaut `oui` seulement si l'année de la
   ligne tombe dans une période de validité de l'opérateur. Une structure créée
   en 2019 n'est donc pas comptée comme opérateur sur une ligne de 2015.
   Le décalage **N-1** est appliqué sur la borne de début de chaque période
   (v5_29), et plusieurs périodes disjointes sont admises pour un même SIREN.
3. **Par famille** (repli) — si l'identification par SIREN a échoué, un motif de
   nom reconnaît les **ARS**, **agences de l'eau** et **parcs nationaux**.

Ce repli ne s'applique **que** si le SIREN n'a pas suffi : il ne peut jamais
contredire une donnée sûre. Les non-opérateurs reçoivent `AUTRE`.

### Étape SGAE — Statut juridique simplifié

Les 269 catégories INSEE sont trop fines pour un tableau de bord. La grille SGAE
(262 codes) les regroupe en catégories exploitables.

> **Jointure sur le CODE, jamais sur le libellé.** Le code à 4 chiffres est
> extrait des parenthèses de `Forme_juridique` (`… (7383)` → `7383`). Les
> libellés ne concordent qu'à **66 %** entre référentiels (« SA » vs « Société
> anonyme ») ; les codes concordent à **100 %**.

### Étape NAF — Libellé d'activité et section

L'API renvoie le **code** NAF mais pas son libellé (vérifié dans son code
source). La jointure se fait donc sur le code avec la nomenclature NAF rév. 2
embarquée (732 sous-classes) — ce qui garantit un libellé **uniforme** quelle
que soit la façon dont la ligne a été identifiée.

Une colonne `Section_NAF` complète le dispositif : elle rattache la division du
code NAF à l'une des **22 sections** de la NAF 2025 (87 divisions référencées),
pour une lecture agrégée par grand secteur d'activité.

### Nom API

Une colonne `Nom API`, placée juste après `Bénéficiaire corrigé`, porte
l'**écriture exacte** du bénéficiaire telle que retournée par l'annuaire des
entreprises. Elle est alimentée avec trois priorités : rapport avec TVA
retenue, puis passe 2, puis autres lignes du rapport (candidat écarté). Elle est
vide quand aucune fiche API n'a été obtenue.

> Son intérêt est la **comparaison** : `Bénéficiaire corrigé` reste le libellé
> FTS nettoyé, `Nom API` donne la dénomination officielle. L'écart entre les
> deux est un bon indicateur de la qualité d'un appariement, et un signal de
> renommage d'entité.
>
> Aucun appel API supplémentaire n'est fait pour l'obtenir : c'est une lecture
> des fiches déjà récupérées (`_fiche_avec_nom`).

---

## 9. Seuils, couleurs et conventions de sortie

### 9.1 Les seuils

| Constante | Valeur | Où | Effet |
|---|---|---|---|
| `SEUIL_SCORE` | **80** | cellule 3 | en dessous : candidat rejeté, rien n'est retenu |
| `SEUIL_HAUTE_CONF` | **92** | cellule 3 | au-dessus : ligne verte ; entre 80 et 92 : jaune |
| `SEUIL_EXACTITUDE_REPLI` | **90** | cellule 4 | exactitude de nom minimale pour le repli `REPLI_EXACT` |
| `SEUIL_FICHIER_TVA` | **96** | cellule 5 | en dessous : la TVA reste au rapport, **pas** dans le fichier |
| `SEUIL_MONTANT_A_CHERCHER` | **300 000 €** | cellule 11 | au-dessus : mention `A CHERCHER` au lieu de `AUTRE` |
| `DELAI_API` | **0,15 s** | cellule 3 | cadence des appels (≈ 7 requêtes/s) |
| `MAX_PAR_REQUETE` | **8** | cellule 3 | candidats examinés par requête |

### 9.2 La règle de remplissage final

Pour une ligne **sans identification retenue** (aucune trouvée, ou score < 96) :

```
        montant cumulé du bénéficiaire ≥ 300 000 € ?
                    │
        ┌───────────┴───────────┐
       OUI                     NON
        │                       │
  « A CHERCHER »            « AUTRE »
  (recherche manuelle)   (non identifié, assumé)
        │
  sauf bénéficiaires « sans TVA possible » → « AUTRE »
```

Le montant est la **somme de tous les contrats** du bénéficiaire, pas celui de
la ligne : un bénéficiaire présent 50 fois pour 10 000 € franchit le seuil.

**Périmètre généralisé depuis la v5_40** — la mention `A CHERCHER` ne concerne
plus la seule TVA. Elle s'applique à : `SIREN`, `SIRET`, `Forme_juridique`,
`Niveau_I/II/III`, `Code_NAF_APE`, `Activite_principale`, `Section_NAF`,
`Etat_entreprise` et `Référentiel SGAE`.

Toutes les **autres** colonnes ajoutées par le pipeline (`Nom API`,
`Adresse réordonnée`, géographie, CFP, opérateurs…) reçoivent `AUTRE` quand la
case est vide.

> **Deux exceptions permanentes.** « République française » et les personnes
> physiques anonymisées (`NATURAL PERSON`, `PERSONNE PRIVÉE`, `Art. 38(7)`…) ne
> reçoivent **jamais** `A CHERCHER` : toutes leurs cases vides passent à
> `AUTRE`. Il n'y a rien à chercher, donc rien à signaler.
>
> **`Référence projet complétée` ne suit pas cette règle** : elle porte son
> propre `NA` dès l'étape 0, et n'entre donc ni dans `A CHERCHER` ni dans
> `AUTRE`.
>
> Le **rapport**, lui, conserve toujours l'intégralité de l'information : ces
> substitutions ne concernent que le fichier livré.

### 9.3 Code couleur

| Couleur | Code | Où | Signification |
|---|---|---|---|
| 🔵 Bleu clair | `BDD7EE` | fichier | **colonne ajoutée** par le pipeline (en-tête + cellules non vides) |
| 🟢 Vert clair | `C6EFCE` | fichier | TVA trouvée en **haute confiance** (score ≥ 92) |
| 🟡 Jaune | `FFEB9C` | fichier | TVA trouvée, confiance moyenne (80 ≤ score < 92) |
| 🟩 Vert vif | `00B050` | fichier | **cellule corrigée** (adresse réordonnée, valeur reprise) |
| 🟠 Orange | `FFC000` | **rapport** | **à chercher à la main** : score < 96 **et** montant ≥ 300 k€ |

> Une cellule **vide** d'une colonne ajoutée reste **non peinte** : c'est voulu,
> cela met en évidence les trous de données.

### 9.4 Les colonnes produites

| Colonne | Position | Contenu |
|---|---|---|
| `Bénéficiaire corrigé` | après le nom | nom nettoyé ou nom officiel |
| `Nom API` | après `Bénéficiaire corrigé` | écriture officielle de l'annuaire |
| `Adresse réordonnée` | après l'adresse | adresse à la norme française, ou adresse d'origine |
| `Référence projet complétée` | après `Project / Contract Reference` | référence numérique du projet, ou `NA` |
| `SIREN` | après `Main registration number` | identifiant INSEE, comparable au SIREN Commission |
| `SIRET` | après la TVA | identifiant d'établissement |
| `Référentiel SGAE` | **avant** `Forme_juridique` | catégorie juridique simplifiée |
| `Forme_juridique` | — | libellé INSEE + code entre parenthèses |
| `Niveau_I`, `Niveau_II`, `Niveau_III` | — | 3 niveaux de la nomenclature juridique |
| `Code_NAF_APE` | — | code d'activité |
| `Activite_principale` | **après** `Code_NAF_APE` | libellé NAF rév. 2 |
| `Section_NAF` | après `Activite_principale` | section NAF 2025 |
| `Etat_entreprise` | — | « En activité » / « Cessée » |
| `Operateur_Etat` | après `Etat_entreprise` | oui / non (selon la période de validité) |
| `Programme_Operateur` | après `Operateur_Etat` | programme chef de file, ou `AUTRE` |
| `Code postal corrigé` | après le CP | CP réparé (zéro initial, etc.) |
| `NUTS2 corrigé` | après `NUTS2` | ancienne région |
| `Metro/RUP/PTOM` | avant `Région_FR` | statut territorial |
| `Région_FR`, `NUTS3 FR`, `NUTS3_Numéro` | — | région actuelle, NUTS3, n° de département |
| `FR/UE/UK/AELE/AUTRE`, `Etats` | après le pays | zone et code ISO |
| `Sous catégorie`, `Période CFP`, `Dépense CFP` | autour de `Programme name` | rattachement budgétaire |
| `N° projet`, `Type de projet` | après la référence LC | identification et nature du projet |

---

## 10. Forcer manuellement un SIREN, un SIRET ou une TVA

C'est la procédure la plus fréquente en exploitation courante. Après une
recherche manuelle (annuaire-entreprises.data.gouv.fr, Kbis, site officiel de
l'entité), vous disposez d'un identifiant certain : voici comment l'imposer au
pipeline **sans toucher au moteur**.

Toutes les tables décrites ci-dessous se trouvent dans la **cellule 4**.

### 10.1 Choisir le bon mécanisme

| Votre situation | Table à éditer | Effet |
|---|---|---|
| Le SIREN trouvé est bon, mais le score est trop bas (< 96) | `_CORRECTIONS_RAPPORT_V2` | score forcé à **100**, la TVA entre au fichier |
| Le SIREN trouvé est **faux**, vous avez le bon | `_CORRECTIONS_RAPPORT_V2` | remplace le SIREN, score 100 |
| Vous avez le bon **SIRET d'établissement** (pas le siège) | `_SIRET_FORCE_BRUT` | force le SIRET précis |
| Le SIREN trouvé est faux et vous n'avez **pas** de remplacement | `_TVA_INTERDITES_BRUT_V2` | bannit définitivement ce couple |
| L'entité n'a **pas** de TVA et ne doit jamais en avoir | `_BENEFICIAIRES_SANS_TVA_PREFIXES` | exclusion, aucun appel API |
| Plusieurs graphies désignent la même entité | `_ALIAS_SIREN_CORRECTIONS` | un SIREN, plusieurs libellés |

> **La TVA ne se saisit jamais à la main.** Elle est **calculée** à partir du
> SIREN par la formule officielle DGFiP :
> `clé = (12 + 3 × (SIREN mod 97)) mod 97`, puis `TVA = "FR" + clé + SIREN`.
> Saisir le SIREN suffit donc : la TVA en découle avec certitude. Si vous ne
> disposez que d'une TVA, le SIREN est simplement ses **9 derniers chiffres**.

### 10.2 Vérifier avant d'écrire (obligatoire)

Tout SIREN/SIRET doit satisfaire la **clé de Luhn**. Ce contrôle détecte les
fautes de frappe et les chiffres inversés. Collez ceci dans une cellule vide :

```python
def controle_luhn(numero):
    """Valide un SIREN (9 chiffres) ou un SIRET (14 chiffres)."""
    n = "".join(c for c in str(numero) if c.isdigit())
    if len(n) not in (9, 14):
        return f"✗ longueur {len(n)} (attendu 9 ou 14)"
    total = 0
    for i, ch in enumerate(reversed(n)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    if total % 10 != 0:
        return "✗ clé de Luhn INVALIDE — vérifiez la saisie"
    if len(n) == 9:
        cle = (12 + 3 * (int(n) % 97)) % 97
        return f"✓ SIREN valide — TVA correspondante : FR{cle:02d}{n}"
    return f"✓ SIRET valide — SIREN associé : {n[:9]}"

# Exemples
print(controle_luhn("130030133"))        # ✓ + TVA calculée
print(controle_luhn("13003013300016"))   # ✓ + SIREN extrait
```

### 10.3 Forcer un SIREN (score porté à 100)

**Table : `_CORRECTIONS_RAPPORT_V2`** — cellule 4.

Ajoutez une entrée `"NOM EXACT DU FTS": "SIREN"` :

```python
_CORRECTIONS_RAPPORT_V2 = {
    "ACCORD RELATIF AUX PECHES DANS LE SUD DE L'OCEAN INDIEN*SOUTHERN INDIAN OCEAN FISHERIES AGREEMENT (SIOFA)": "823504279",
    "AGRISUD INTERNATIONAL INSTITUT INTERNATIONAL POUR APPUI AU DEVELOPPEMENT ASSOCIATION*": "390364776",
    # ─── votre ajout ───────────────────────────────────────────
    "MON BENEFICIAIRE TEL QU'ECRIT DANS LE FTS*SIGLE": "123456782",
    # ───────────────────────────────────────────────────────────
}
```

**Trois précautions impératives :**

1. **Le nom doit être copié EXACTEMENT** depuis la colonne `Nom_FTS` du rapport,
   étoile et sigle compris. La correspondance est **exacte après
   normalisation** (majuscules, accents et ponctuation neutralisés), **jamais
   en sous-chaîne**.

   > *Pourquoi cette sévérité ?* Une correspondance partielle sur « THALES »
   > capterait « THALES ALENIA SPACE », « THALES DIS FRANCE »… et leur
   > attribuerait toutes le même SIREN. La correspondance exacte est la seule
   > qui garantisse l'absence d'effet de bord.

2. **Ne pas oublier la virgule** en fin de ligne (c'est un dictionnaire Python).

3. **Attention aux apostrophes** : si le nom contient `'`, utilisez des
   guillemets doubles pour délimiter la chaîne (comme dans les exemples).

**Effet obtenu** : statut `TROUVE`, stratégie `ALIAS_SIREN`, score `100`, TVA
recalculée, SIRET et forme juridique récupérés depuis la fiche SIRENE du siège,
et donc **présence garantie dans le fichier final** (100 ≥ 96).

### 10.4 Forcer un SIRET d'établissement

**Table : `_SIRET_FORCE_BRUT`** — cellule 4.

Cas d'usage : l'API renvoie le SIRET du **siège**, mais vous savez que le
bénéficiaire réel est un **établissement particulier** (délégation régionale,
antenne locale). C'est aussi le recours quand la désambiguïsation automatique
par code postal n'a rien donné (voir §8).

```python
_SIRET_FORCE_BRUT = {
    "ASSOCIATION PREMIERS PLANS*FESTIVAL PREMIERS PLANS": "34089242700028",
    # ─── votre ajout ───────────────────────────────────────────
    "MON BENEFICIAIRE*": "12345678200002",   # SIRET de l'établissement
    # ───────────────────────────────────────────────────────────
}
```

Le SIREN est **déduit automatiquement** des 9 premiers chiffres — inutile de
l'ajouter ailleurs. Le SIRET forcé n'est appliqué que si le SIREN du résultat
correspond bien : ce garde-fou évite d'attribuer un SIRET à la mauvaise entité.

### 10.5 Bannir un faux positif

**Table : `_TVA_INTERDITES_BRUT_V2`** — cellule 4.

Cas d'usage : le moteur trouve un SIREN qui **n'est pas** celui du bénéficiaire,
et vous n'avez pas de remplacement.

```python
_TVA_INTERDITES_BRUT_V2 = {
    "ECOLE NATIONALE D'ADMINISTRATION*ENA": ["485241327"],
    # ─── votre ajout ───────────────────────────────────────────
    "MON BENEFICIAIRE*": ["900000001", "900000019"],   # liste possible
    # ───────────────────────────────────────────────────────────
}
```

**Effet** : quel que soit le chemin par lequel le moteur retrouverait ce SIREN
pour ce bénéficiaire, le résultat est annulé (`NON_TROUVE`, stratégie
`FAUX_POSITIF_ECARTE`), ce qui est **visible dans le rapport**.

> **La portée est le couple (nom, SIREN), pas le SIREN seul.** Le SIREN banni
> reste attribuable à d'autres bénéficiaires — c'est voulu : un même SIREN peut
> être un faux positif ici et la bonne réponse ailleurs.

> **Pas de « re-recherche automatique ».** Le moteur est déterministe : relancé
> sur le même nom, il proposerait le même candidat banni. Ces lignes basculent
> donc en recherche manuelle (`A CHERCHER` si le montant ≥ 300 k€, sinon
> `AUTRE`).

### 10.6 Exclure une entité qui ne peut pas avoir de TVA

**Table : `_BENEFICIAIRES_SANS_TVA_PREFIXES`** — cellule 4.

```python
_BENEFICIAIRES_SANS_TVA_PREFIXES = ("REPUBLIQUEFRANCAISE",)
```

Le préfixe est écrit **normalisé** : majuscules, sans accents, **sans espaces
ni ponctuation**. `« République française »` devient `REPUBLIQUEFRANCAISE`.

**Effet** : statut `EXCLU` / `SANS_TVA_MANUEL`, **aucun appel API** (gain
important quand l'entité revient des milliers de fois), aucune entrée en Passe 0
même si un *Main Registration* est présent, et jamais de mention `A CHERCHER` —
puisqu'il n'y a rien à chercher.

### 10.7 Regrouper plusieurs graphies sous un même SIREN

**Table : `_ALIAS_SIREN_CORRECTIONS`** — cellule 4. Ici la clé est le **SIREN**
et la valeur une **liste de libellés** :

```python
_ALIAS_SIREN_CORRECTIONS = {
    "130011836": ["AUTORITE NATIONALE DES JEUX", "AUTORITE REGULATION JEUX EN LIGNE"],
    # ─── votre ajout ───────────────────────────────────────────
    "123456782": ["MON ENTITE", "MON ENTITE ANCIEN NOM", "SIGLE DE MON ENTITE"],
    # ───────────────────────────────────────────────────────────
}
```

Utile pour les **renommages** (Pôle emploi → France Travail) et les entités
connues sous plusieurs appellations. Vous pouvez compléter
`_NOM_OFFICIEL_PAR_SIREN` pour fixer le libellé affiché en sortie :

```python
_NOM_OFFICIEL_PAR_SIREN = {
    "130005481": "FRANCE TRAVAIL",
    "123456782": "NOM OFFICIEL A AFFICHER",
}
```

### 10.8 Ajouter un opérateur de l'État

**Table : bloc `OPERATEURS_ETAT`** — cellule 2. Attention, c'est du **JSON**,
pas du Python : la syntaxe est plus stricte.

**Trois** endroits à compléter depuis la v5_27 :

```json
  "programmes": {
      "824544514": "113 – Paysages, eau et biodiversité",
      "130030133": "102 – Accès et retour à l'emploi"
  },
  "sirens": {
      "824544514": "AGENCE DE L'EAU LOIRE-BRETAGNE",
      "130030133": "GIP Plateforme de l’inclusion"
  },
  "periodes": {
      "824544514": [[2000, 2026]],
      "130030133": [[2021, 2026]]
  }
```

> **`periodes`** porte les années de validité de l'opérateur, sous forme de
> listes de bornes `[début, fin]` — plusieurs périodes disjointes sont admises.
> Sans période, `Operateur_Etat` ne pourra pas être évalué correctement selon
> l'année de la ligne.

> **Piège JSON n°1 — la virgule.** Chaque entrée sauf la dernière doit se
> terminer par une virgule. Une virgule oubliée provoque
> `Expecting ',' delimiter` et **le chargement de tous les référentiels
> échoue**. C'est l'erreur la plus fréquente.

> **Piège JSON n°2 — pas de commentaires.** Le JSON n'accepte pas `#` ni `//`.

> **Piège n°3 — les trois tables.** Ajouter le SIREN dans `sirens` sans
> l'ajouter dans `programmes` donne un opérateur reconnu mais **sans
> programme** (case vide). Ajouter dans `programmes` seulement ne le marque pas
> comme opérateur. Omettre `periodes` fausse l'évaluation par millésime.

**Contrôle après édition** — relancez la cellule 13 et vérifiez que le compteur
d'opérateurs a augmenté de 1 (443 SIREN dans la v5_41). S'il est absent ou à 0,
le JSON est cassé.

### 10.9 Vérifier que votre forçage a bien pris

Après édition, **réexécutez la cellule concernée** puis la cellule 13, et testez
sans lancer tout le pipeline :

```python
# 1. la correction est-elle chargée ?
from_norm = _norm_alias("MON BENEFICIAIRE TEL QU'ECRIT DANS LE FTS*SIGLE")
print("SIREN forcé :", _ALIAS_SIREN_EXACT.get(from_norm, "❌ ABSENT"))

# 2. combien de corrections au total ?
print(f"{len(_ALIAS_SIREN_EXACT)} corrections | "
      f"{len(_SIRET_FORCE)} SIRET forcés | "
      f"{sum(len(v) for v in _TVA_INTERDITES.values())} paires bannies")

# 3. l'opérateur est-il reconnu ?
print("Opérateur :", OPERATEURS_ETAT_SIRENS.get("130030133", "❌ ABSENT"))
print("Programme :", OPERATEURS_ETAT_PROG.get("130030133", "❌ ABSENT"))
```

Si `❌ ABSENT` s'affiche alors que vous avez bien ajouté l'entrée, la cause est
presque toujours l'une de ces trois :

1. la cellule modifiée n'a pas été réexécutée ;
2. le nom ne correspond pas **exactement** à celui du rapport (espace double,
   accent, étoile manquante) ;
3. une virgule manque dans le dictionnaire ou le JSON.

---

## 11. Ajouter ou modifier un référentiel

Les référentiels sont des blocs **JSON** dans la cellule 2 (voir §10.8 pour la
syntaxe et les pièges). Principes généraux :

| Référentiel | Quand y toucher |
|---|---|
| `OPERATEURS_ETAT` | nouvel opérateur, changement de programme ou de période |
| `SOUS_CATEGORIES` | programme signalé `NA` en fin d'étape 0 |
| `PAYS_ALIAS` | pays signalé `AUTRE` en fin d'étape 0 |
| `REF_SGAE` | nouvelle catégorie juridique à classer |
| `GEO_FRANCE` | évolution territoriale (rare) |
| `NAF_REV2`, `NAF2025_SECTIONS` | changement de nomenclature INSEE (rare) |
| `VILLES_FR` | commune manquante — **attention aux homonymes** |

**Règle absolue** : toute valeur ajoutée provient d'une **source officielle**
et est vérifiée (Luhn pour les identifiants, recoupement web en cas de doute).
Ne jamais saisir de mémoire.

> **Vérifier avant d'intégrer un alias pays.** La campagne v5_33 a été validée
> ligne à ligne contre le fichier FTS réel (volumes observés à l'appui) ; la
> v5_34, portant sur des territoires rares, ne l'a **pas** été faute
> d'occurrences — ces alias restent à confirmer sur un fichier où ces pays
> apparaissent. Le doute est documenté plutôt que masqué.

**Cas particulier des homonymes** : n'ajoutez une commune à `VILLES_FR` que si
son nom est **non ambigu** au niveau national. Ajouter « SAINT-DENIS »
affecterait arbitrairement toutes les lignes homonymes à un seul département,
faussant la répartition Metro/RUP/PTOM.

---

## 12. Le cycle de correction par le rapport

C'est le mécanisme d'amélioration continue du pipeline. Il boucle ainsi :

```
   ①  exécution du pipeline
            ↓
   ②  RAPPORT : toutes les recherches, avec score et stratégie
            ↓
   ③  relecture humaine — priorité aux lignes ORANGE
       (score < 96 et montant ≥ 300 k€)
            ↓
   ④  annotation d'une colonne CORRECTION dans le rapport
            ↓
   ⑤  intégration des corrections dans les tables (§10)
            ↓
   ⑥  réexécution : les cas corrigés sortent à 100
```

### 12.1 Les annotations reconnues

| Annotation | Signification | Table de destination |
|---|---|---|
| `CORRECT` | le SIREN trouvé est bon, malgré un score faible | `_CORRECTIONS_RAPPORT_V2` |
| `CORRIGE` | le SIREN était faux, le bon est saisi dans la colonne SIREN | `_CORRECTIONS_RAPPORT_V2` |
| `CORRIGE SIRET` | le bon SIRET d'établissement est saisi | `_SIRET_FORCE_BRUT` |
| `CORRIGE TVA` | la bonne TVA est saisie (le SIREN en est déduit) | `_CORRECTIONS_RAPPORT_V2` |
| `TROUVE` | identifiant trouvé manuellement | `_CORRECTIONS_RAPPORT_V2` |
| `False` | le SIREN trouvé est faux, sans remplacement | `_TVA_INTERDITES_BRUT_V2` |

### 12.2 Points de vigilance constatés

- **Le SIRET affiché sur une ligne `CORRIGE` est l'ANCIEN** (celui du mauvais
  appariement) : l'ignorer, le bon SIRET est celui du siège du nouveau SIREN.
- **Nettoyer les SIREN collés** : les copier-coller depuis un navigateur
  introduisent des `\n`, des espaces (`« 254 401 839 »`) — à retirer.
- **Vérifier les conflits croisés** : un même SIREN banni pour une graphie et
  forcé pour une autre signale une erreur d'annotation à trancher.
- **Toujours contrôler la clé de Luhn** avant intégration (§10.2).
- **Comparer `SIREN` et `Main registration number`** : depuis la v5_32, les deux
  colonnes sont côte à côte dans le fichier. Un écart entre le SIREN de la
  Commission et celui retenu par le pipeline est un cas à examiner en priorité.

---

## 13. Tester une modification sans rien casser

### 13.1 La vérification obligatoire du moteur figé

À exécuter **avant toute livraison**, dans une cellule vide :

```python
import json, re

def corps_fonction(source, nom):
    """Extrait le corps d'une fonction, hors commentaires qui la précèdent."""
    m = re.search(rf'^def {nom}\(.*?(?=^\S)', source, re.M | re.S)
    return m.group(0) if m else None

# Chargez ici la version de référence v5_1 et la version courante
reference = json.load(open("v5_1.ipynb"))
courante  = json.load(open("FTS_France_PREPARATION_ENRICHISSEMENT_v5_41.ipynb"))
src_ref = "\n".join("".join(c["source"]) for c in reference["cells"])
src_cur = "\n".join("".join(c["source"]) for c in courante["cells"])

for fn in ["rechercher_tva", "_scorer", "_generer_requetes",
           "_appeler_api", "_est_exclu"]:
    identique = corps_fonction(src_ref, fn) == corps_fonction(src_cur, fn)
    print(f"  {fn:20s} {'✓ identique' if identique else '✗✗ MODIFIÉ ✗✗'}")
```

Les cinq lignes doivent afficher `✓ identique`. Une seule croix invalide la
livraison.

> **Note** : les commentaires **au-dessus** d'une fonction ne comptent pas dans
> son corps — c'est pourquoi la v5_19 a pu documenter le moteur sans rompre la
> garantie.

### 13.2 Tester hors ligne

Pour valider une modification sans consommer d'appels API :

```python
import types, sys, requests

# Simuler l'absence de Colab et couper le réseau
g = types.ModuleType("google"); gc = types.ModuleType("google.colab")
gc.files = types.SimpleNamespace(upload=lambda: {}, download=lambda x: None)
g.colab = gc
sys.modules["google"] = g; sys.modules["google.colab"] = gc
requests.get = lambda *a, **k: (_ for _ in ()).throw(Exception("offline"))
```

Puis remplacez les fonctions d'accès réseau par des doublures **aux signatures
exactes** :

```python
_appeler_api = lambda query, cp, nom_ref, adr_ref, ville_ref, session, \
                      exige_collectivite=False: None

infos_par_siren = lambda siren, session, essais=4: {
    "siren": siren, "siret": siren + "00017",
    "forme_juridique": "Association déclarée (9220)",
    "code_naf": "94.99Z", "etat": "En activité",
    "adresse_api": "1 RUE DE TEST 75001 PARIS",
    "cp_api": "75001", "ville_api": "PARIS",
    "niveau_i": "9", "niveau_ii": "92", "niveau_iii": "9220",
}
```

### 13.3 Tester la dérivation des références sans lancer le pipeline

La fonction d'extraction est pure : elle se teste isolément.

```python
for libelle in [
    "101177660 - MAKE-A-THEK - MODULAR LIBRARY MAKERSPACES",
    "KBBE-2013 613960 SMARTBEES",
    "2012-DE-17022-S PLANUNG DES NEUBAUS",
    "Provisional budgetary commitment covering routine administrative expenditure",
]:
    print(extraire_reference_objet(libelle), "|", libelle[:55])
```

Attendu : `('101177660', 'NUM_EN_TETE')`, `('613960', 'NUM_APRES_CODE_APPEL')`,
puis `('', '')` pour les deux derniers (code non numérique, puis aucun nombre).

### 13.4 Contrôles recommandés

| Contrôle | Comment |
|---|---|
| Moteur intact | §13.1 |
| Référentiels chargés | compteurs de la cellule 13 |
| Colonnes bien placées | `list(df.columns).index("…")` |
| Colonnes bien peintes | relire l'export avec `openpyxl` |
| Non-régression des corrections | vérifier que d'anciens cas forcés sortent toujours à 100 |
| Cohérence des références | sur un fichier 2025, comparer `Project / Contract Reference` et la valeur qu'aurait produite la dérivation |

---

## 14. Dépannage

| Symptôme | Cause probable | Solution |
|---|---|---|
| `Expecting ',' delimiter` | virgule manquante dans un bloc JSON de la cellule 2 | ajoutez la virgule ; le message donne la ligne |
| `NameError: ... is not defined` | une cellule de définition n'a pas été exécutée | relancer les cellules dans l'ordre |
| Compteur de référentiel à `0` | bloc JSON cassé | vérifier virgules et guillemets |
| `FileNotFoundError` sur le rapport | aucune TVA manquante → rapport non créé | comportement normal |
| Beaucoup de `HTTP 429` | cadence trop élevée | augmenter `DELAI_API` (0.15 → 0.25) |
| Session Colab interrompue | traitement trop long | découper le fichier en lots |
| Une correction reste sans effet | nom non exact, cellule non réexécutée, ou virgule manquante | voir §10.9 |
| Un opérateur reste à `non` | le SIREN est absent du référentiel, **ou l'année de la ligne est hors période de validité** | vérifier §10.8, notamment `periodes` |
| `Metro/RUP/PTOM` vide | ni CP, ni SIREN, ni ville exploitables | normal : on n'invente pas de territoire |
| Colonne ajoutée non bleue | absente de la liste de coloriage | ajoutez-la dans `exporter_pipeline` (cellule 11) |
| Passe 0 sans effet | colonne `Main registration` absente (millésime < 2025), ou valeurs non conformes à un SIREN | comportement normal, la Passe 1 prend le relais |
| Passe mémoire sans effet | `correspondance_siren.json` non déposé et bénéficiaires absents du socle embarqué | normal au premier traitement d'un périmètre nouveau |
| `Référence projet complétée` majoritairement à `NA` | les libellés ne portent pas de référence numérique en tête | lire les 15 exemples affichés en fin d'étape 0 avant de conclure |

---

## 15. Glossaire

| Terme | Définition |
|---|---|
| **FTS** | *Financial Transparency System* — base publique des bénéficiaires de fonds européens en gestion directe et indirecte |
| **Gestion directe / indirecte** | fonds gérés par la Commission ou ses agences (~25 % du budget) — périmètre du FTS |
| **Gestion partagée** | fonds gérés par les États membres (cohésion, PAC, ~75 %) — **hors** FTS |
| **Kohesio** | base de la Commission recensant les projets de la politique de cohésion (gestion partagée) |
| **CFP** | Cadre financier pluriannuel — budget de l'UE sur 7 ans |
| **PPNR** | Plan de partenariat national et régional — futur fonds unique de gestion partagée (2028-2034) |
| **Main Registration** | champ FTS (millésime 2025+) portant le numéro de registre national du bénéficiaire — un SIREN pour la France |
| **Mémoire de correspondance** | table (nom, département) → SIREN, constituée sur un millésime et réutilisée sur les autres |
| **SIREN** | identifiant à 9 chiffres d'une **unité légale** française |
| **SIRET** | identifiant à 14 chiffres d'un **établissement** (SIREN + NIC à 5 chiffres) |
| **SIRENE** | répertoire INSEE des entreprises et établissements |
| **NAF / APE** | nomenclature d'activité française (code + libellé) ; la **section** est le niveau le plus agrégé |
| **NUTS** | nomenclature territoriale européenne : NUTS2 ≈ anciennes régions, NUTS3 ≈ départements |
| **RUP** | Région ultrapériphérique (art. 349 TFUE) — **dans** l'UE |
| **PTOM** | Pays et territoire d'outre-mer (annexe II TFUE) — **hors** UE |
| **Clé de Luhn** | somme de contrôle validant un SIREN ou un SIRET |
| **Score** | note 0-100 de confiance de l'appariement |
| **Moteur figé** | les 5 fonctions de recherche, non modifiables |
| **Enveloppe** | `rechercher_tva_plus`, couche modifiable autour du moteur |

---

## 16. Sources et références

### Données et API

| Source | Usage |
|---|---|
| [Système de transparence financière (FTS)](https://ec.europa.eu/budget/financial-transparency-system/) | fichier source |
| [API Recherche d'entreprises](https://recherche-entreprises.api.gouv.fr/docs/) | identification SIREN/SIRET |
| [Annuaire des entreprises](https://annuaire-entreprises.data.gouv.fr/) | vérification manuelle |
| [INSEE — catégories juridiques](https://www.insee.fr/fr/information/2028129) | nomenclature `cj_septembre_2022` |
| [INSEE — NAF rév. 2](https://www.insee.fr/fr/information/2120875) | libellés d'activité |
| [INSEE — NAF 2025](https://www.insee.fr/fr/information/8244015) | sections et divisions |

---

## 17. Nouveautés depuis la v5_19

Récapitulatif des évolutions à connaître, de la plus récente à la plus ancienne.
Le **moteur de recherche est resté strictement inchangé** sur toute la période.

| Version | Apport | Section |
|---|---|---|
| **v5_41** | Colonne `Référence projet complétée` — référence numérique dérivée du libellé de l'objet quand la Commission ne la fournit pas | §5.7 |
| **v5_40** | `Nom API` complétée par la passe 2 · colonne `Adresse réordonnée` · `AUTRE` / `A CHERCHER` généralisés à toutes les colonnes ajoutées | §8, §9.2 |
| **v5_38 – v5_39** | Colonne `Nom API` : écriture officielle de l'annuaire, y compris pour les Passes 0 et mémoire | §8 |
| **v5_31 – v5_37** | **Mémoire de correspondance inter-millésimes** : socle embarqué, repli « nom seul », propagation intra-fichier | §8 |
| **v5_32** | Colonne `SIREN` repositionnée face au *Main Registration* · désambiguïsation d'établissement par code postal (best-effort) | §8, §12.2 |
| **v5_33 – v5_34** | 13 programmes CFP 21-27 (198 au total) · 13 alias pays, dont les troncatures `« Republ »` et `« Republic o »` | §5.3, §5.6 |
| **v5_30** | **Passe 0** : exploitation du champ *Main registration number* du millésime 2025 | §8 |
| **v5_27 – v5_29** | `Operateur_Etat` conditionné à une **période de validité** (oui/non), 438 périodes, décalage N-1 sur la borne de début | §8, §10.8 |
| **v5_23 – v5_26** | Référentiel des opérateurs de l'État porté à **443 SIREN** (annexe Jaune complète) | §4.2 |
| **v5_20 – v5_22** | Colonne `Section_NAF` (NAF 2025 : 22 sections, 87 divisions) | §8 |

### Points de vigilance introduits par ces évolutions

- **La mémoire de correspondance est un actif** : conservez
  `correspondance_siren.json` d'une exécution à l'autre, c'est lui qui réduit le
  volume d'appels réseau sur les millésimes anciens.
- **`Operateur_Etat` répond désormais `oui`/`non`** et dépend de l'année de la
  ligne : un tableau de bord construit sur l'ancien `1`/`0` doit être adapté.
- **Trois tables à compléter** pour un nouvel opérateur, et non plus deux
  (`sirens`, `programmes`, `periodes`).
- **Les alias pays de la v5_34 ne sont pas empiriquement vérifiés** : à
  confirmer sur un fichier où ces territoires apparaissent réellement.
- **La désambiguïsation d'établissement est best-effort** : en cas d'échec, le
  SIRET du siège est conservé. Pour un établissement précis, passer par
  `_SIRET_FORCE_BRUT` (§10.4).
