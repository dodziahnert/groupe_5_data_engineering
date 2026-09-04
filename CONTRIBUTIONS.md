# Contributions et choix techniques

Ce document trace la contribution de chaque membre, les choix techniques du groupe et les relectures croisees.

## Tableau des responsabilites et relectures

| Partie | Question | Responsable | Relecteur |
|--------|----------|-------------|-----------|
| 1 | Structure du projet et build.sbt | Membre A | Membre C |
| 2 | Ingestion, validation, rapport de qualite | Membre A | Membre B |
| 3 | Transformations avancees | Membre B | Membre A |
| 4 | Analytique business | Membre C | Membre B |
| 5 | Optimisations Spark | Membre C | Membres A et B |
| 6 | Application principale | Membre C | Les 3 membres |
| 7 | Configuration externalisee | Membre A | Membre C |

## Charge de travail estimee et difficultes

Les estimations ci-dessous sont a ajuster par chaque membre.

### Membre A - Mamoudou BAH (environ 18 h)
Difficultes : incompatibilite entre Spark 3.5.1 et Java 21 au demarrage (resolue par le passage a Java 17 et l'ajout d'un fichier .jvmopts) ; typage du champ age lu en entier large depuis le JSON ; definition du schema explicite des transactions.

### Membre B - AHNERT Dodzi (environ 16 h)
Difficultes : robustesse de l'UDF temporelle face aux horodatages nuls ou mal formes ; typage strict du fenetrage glissant sur 7 jours (cast en Long pour rangeBetween) ; choix des types de jointure pour ne pas perdre de transactions.

### Membre C - BOUKOYI MOUTSASSI Rhupthur Jevainaitre (environ 17 h)
Difficultes : gestion des valeurs nulles et des timestamps corrompus lors de l'assemblage du pipeline ; parametrage des optimisations Spark via la configuration ; orchestration et execution par etape.

## Decisions techniques du groupe

1. **Versions Scala 2.12.18 et Spark 3.5.1** : Spark est historiquement construit sur Scala 2.12 ; ce couple est le plus stable et le mieux documente, ce qui limite les mauvaises surprises.

2. **Java 17** : Spark 3.5.1 ne fonctionne pas correctement avec Java 21 (acces bloque a un module interne). Java 17 est la version supportee ; un fichier .jvmopts avec des options d'ouverture de modules complete ce choix.

3. **Generation du jar avec sbt-assembly** : produit un jar autonome, plus simple a lancer avec spark-submit qu'un jar leger necessitant de fournir les dependances a l'execution. Spark est declare "provided" pour ne pas alourdir le jar.

4. **Strategie de jointure (left join)** : la table des transactions est la table principale ; les utilisateurs, produits et marchands sont joints en left join afin de conserver chaque transaction, meme lorsqu'une fiche de reference est absente, et de ne pas fausser les analyses.

5. **Separation des lignes valides et rejetees** : plutot que de simplement filtrer, chaque validation renvoie les lignes valides et les lignes rejetees, ces dernieres enrichies d'un motif de rejet. Ce choix permet de produire un rapport de qualite exploitable.

6. **Formats de sortie CSV et Parquet** : les resultats sont sauvegardes dans les deux formats. Le CSV est lisible par une equipe metier, le Parquet est compresse et reutilisable efficacement par d'autres traitements.

7. **Configuration externalisee** : tous les chemins, seuils de validation et parametres Spark sont dans application.conf, avec un mecanisme de valeur par defaut si une cle est absente. Aucun de ces parametres n'est code en dur.

## Ingestion, validation et qualite (Membre A - Mamoudou BAH)

Ingestion multi-format : schema explicite pour transactions.csv, inference pour merchants.csv, lecture native du JSON et du Parquet. Validation des quatre datasets selon les regles du sujet (montant, timestamp, age, revenu, prix, note, commission), avec separation valides/rejetes et motif de rejet. Rapport de qualite en CSV et Parquet, contenant par dataset le nombre de lignes lues, valides, rejetees, le taux de rejet et le nombre total de valeurs nulles. Externalisation complete de la configuration.

## Transformation et enrichissement (Membre B - AHNERT Dodzi)

La table des transactions est la table principale : les utilisateurs, produits et marchands sont joints avec des `left join` sur leurs identifiants. Ce choix conserve chaque transaction, même lorsqu'une fiche de reference est absente, afin de ne pas perdre d'evenements dans les indicateurs temporels et de controle.

La transformation ajoute les caracteristiques du timestamp, les rangs et compteurs par utilisateur, les fenetres glissantes de sept jours, le delai depuis le precedent achat et les indicateurs de suspicion. Les moyennes historiques excluent la transaction courante pour eviter qu'elle ne se compare a elle-meme.

## Analyse et optimisation (Membre C - BOUKOYI MOUTSASSI Rhupthur Jevainaitre)

### Analyses metier
Les indicateurs marchands agregent le chiffre d'affaires, les commissions, le nombre de transactions et de clients uniques, ainsi que le nombre de transactions suspectes. Un classement par categorie et par region est calcule au moyen de fenetres. Des analyses de cohortes de retention et de detection de fraude par marchand completent le rapport.

### Optimisations Spark
Plusieurs optimisations conditionnelles ont ete mises en place et pilotees par la configuration : un broadcast join sur la table des marchands (petite table de reference) pour eviter un shuffle couteux, la mise en cache et le persist en memoire et disque des donnees reutilisees, et un nombre de partitions de shuffle configurable. Ces reglages sont actives ou desactives via la configuration, ce qui permet de comparer les performances.

### Orchestration
Le point d'entree MainApp enchaine les trois etapes du pipeline (ingestion et validation, transformation, analyse) et gere les cas particuliers rencontres a l'assemblage, notamment les valeurs nulles et les timestamps corrompus.

### Lancement par etape
MainApp accepte un argument en ligne de commande (ingestion, transformation, analytics, ou all par defaut) qui permet de lancer une seule etape du pipeline sans executer le reste, afin de tester chaque module independamment. Les dependances entre etapes sont respectees : chaque etape rejoue ce dont elle a besoin en amont. Chaque etape journalise son heure de debut, son heure de fin et sa duree. Un argument inconnu affiche un message d'aide listant les valeurs acceptees, sans lever d'exception non geree.

## Relecture croisee

Chaque module a ete relu par un autre membre que son auteur avant fusion dans la branche principale.

- **Partie 1 (structure, build.sbt)** relue par Membre C, le 30 aout 2026. Remarque : arborescence conforme au sujet, dependances correctes.
- **Partie 2 (ingestion, validation, qualite)** relue par Membre B, le 1 septembre 2026. Remarque : regles de validation conformes, rapport de qualite complet.
- **Partie 3 (transformations)** relue par Membre A, le 2 septembre 2026. Remarque : jointures et fenetrages corrects, UDF robuste.
- **Partie 4 (analytique)** relue par Membre B, le 3 septembre 2026. Remarque : KPI et cohortes coherents.
- **Partie 5 (optimisations)** relue par Membres A et B, le 3 septembre 2026. Remarque : optimisations pilotees par la configuration.
- **Partie 6 (application principale)** relue par les 3 membres, le 4 septembre 2026. Remarque : pipeline complet fonctionnel, execution par etape validee.
- **Partie 7 (configuration)** relue par Membre C, le 4 septembre 2026. Remarque : aucun parametre code en dur, valeurs par defaut presentes.
