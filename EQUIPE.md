# Equipe - Groupe 5

Projet Data Engineering : pipeline de traitement de donnees e-commerce en Scala et Apache Spark.

## Membres

| Membre | Nom et prenom | E-mail | Nom configure dans Git |
|--------|---------------|--------|------------------------|
| A | Mamoudou BAH | bahmahmoud556@gmail.com | Mamoudou BAH |
| B | AHNERT Dodzi | dodzi.ahnert@hotmail.fr | Dodzi AHNERT |
| C | BOUKOYI MOUTSASSI Rhupthur Jevainaitre | rjmboukoyi@gmail.com | Rhupthur Boukoyi |

## Roles et questions traitees

### Membre A - Mamoudou BAH
Role : Data Ingestion & Platform Engineer.
Questions traitees : Partie 1 (structure du projet, build.sbt), Partie 2 (ingestion multi-format, validation des donnees, gestion d'erreurs, rapport de qualite), Partie 7 (configuration externalisee). Redaction du README.

### Membre B - AHNERT Dodzi
Role : Transformation & Enrichissement.
Questions traitees : Partie 3 (UDF d'extraction de caracteristiques temporelles, enrichissement des donnees, analyses par fenetre glissante, detection de transactions suspectes).

### Membre C - BOUKOYI MOUTSASSI Rhupthur Jevainaitre
Role : Analyse & Optimisation.
Questions traitees : Partie 4 (KPI marchands, cohortes de retention), Partie 5 (optimisations Spark : cache, persist, broadcast, partitions de shuffle), Partie 6 (application principale et orchestration, execution modulaire par etape).

## Organisation du travail

Le travail a ete organise avec Git : une branche par tache, des commits reguliers avec des messages explicites, et une pull request par fonctionnalite avant fusion dans la branche principale. Chaque membre a configure son nom et son e-mail dans Git afin que ses commits soient identifiables dans l'historique. Chaque partie a ete relue par un autre membre avant integration (voir CONTRIBUTIONS.md).

## Environnement technique

- Langage : Scala 2.12.18
- Moteur : Apache Spark 3.5.1
- Compilation : SBT 1.9.9
- Execution : Java 17
