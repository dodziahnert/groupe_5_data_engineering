# Equipe - Groupe 5

Projet Data Engineering : pipeline de traitement de donnees e-commerce en Scala et Apache Spark.

## Membres et repartition des roles

| Membre | Nom | Role | Parties | Fichiers principaux |
|--------|-----|------|---------|---------------------|
| A | Mamoudou BAH | Data Ingestion & Platform Engineer | 1, 2, 7 | build.sbt, application.conf, case classes, DataIngestion, DataValidation, DataQualityReport, README |
| B | AHNERT Dodzi | Transformation & Enrichissement | 3, 4 | DataTransformation, TimeFeatures |
| C | BOUKOYI MOUTSASSI Rhupthur Jevainaitre | Analyse & Optimisation | 5, 6 | Analytics, SparkOptimizations, MainApp |

## Detail des responsabilites

### Membre A - Mamoudou BAH
Mise en place des fondations du projet : structure des dossiers, configuration de compilation (build.sbt, sbt-assembly), externalisation des parametres dans application.conf. Definition des case classes decrivant les quatre sources de donnees. Lecture typee des fichiers (ingestion), validation des donnees selon des regles metier avec separation des lignes valides et rejetees, et production du rapport de qualite des donnees. Redaction du README.

### Membre B - AHNERT Dodzi
Transformation et enrichissement des donnees : jointures entre les transactions et les tables de reference (utilisateurs, produits, marchands), extraction de caracteristiques temporelles a partir du timestamp brut, calcul de rangs et compteurs par utilisateur, fenetres glissantes et indicateurs de suspicion. Optimisation de la fonction de transformation.

### Membre C - BOUKOYI MOUTSASSI Rhupthur Jevainaitre
Analyses metier : indicateurs sur les marchands (chiffre d'affaires, commissions, clients uniques, rangs par categorie et par region), cohortes de retention et detection de fraude. Optimisations Spark (broadcast join, cache et persist conditionnels, nombre de partitions de shuffle configurable). Orchestration complete du pipeline dans MainApp.

## Organisation du travail

Le travail a ete organise avec Git : une branche par tache, un commit par etape avec un message explicite, et une pull request par fonctionnalite avant fusion dans la branche principale. Chaque partie a ete relue par un autre membre avant integration.

## Environnement technique

- Langage : Scala 2.12.18
- Moteur : Apache Spark 3.5.1
- Compilation : SBT 1.9.9
- Execution : Java 17
