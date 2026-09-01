# EcommerceAnalytics

Pipeline de traitement de donnees e-commerce developpe en Scala avec Apache Spark.

Le projet lit des donnees de ventes en ligne (transactions, utilisateurs, marchands, produits), controle leur qualite, puis prepare des analyses. Ce document decrit la partie actuellement disponible : l'ingestion, la validation et le rapport de qualite des donnees.

## Prerequis

- Java 17 (le projet ne fonctionne pas avec Java 21)
- SBT 1.9.9
- Scala 2.12.18 et Spark 3.5.1 (telecharges automatiquement par SBT)

Pour verifier votre version de Java :

```
java -version
```

## Structure du projet

```
groupe_5_data_engineering/
├── build.sbt                        configuration du projet et des dependances
├── .jvmopts                         options java pour faire tourner spark sur java 17
├── project/
│   ├── build.properties             version de SBT
│   └── plugins.sbt                  plugin sbt-assembly
└── src/main/
    ├── resources/
    │   ├── application.conf          chemins des fichiers et parametres
    │   └── data/                     donnees sources (csv, json, parquet)
    └── scala/com/ecommerce/
        ├── models/                   case classes (Transaction, User, Product, Merchant)
        ├── utils/                    session spark et lecture de la configuration
        └── analytics/                ingestion, validation, rapport qualite
```

## Donnees sources

| Fichier              | Format  | Contenu                        |
|----------------------|---------|--------------------------------|
| transactions.csv     | CSV     | Les achats effectues           |
| users.json           | JSON    | Les profils des clients        |
| merchants.csv        | CSV     | Les marchands                  |
| products.parquet     | Parquet | Le catalogue de produits       |

## Compilation

Pour compiler le projet :

```
sbt compile
```

## Execution

Pour lancer le programme :

```
sbt run
```

Le programme lit les donnees, applique les regles de validation, affiche un recapitulatif de qualite et sauvegarde deux rapports au format CSV dans le dossier `output/`.

## Regles de validation

Une ligne est consideree comme valide si elle respecte toutes les regles ci-dessous. Sinon elle est rejetee, avec le motif du rejet.

Transactions :

- transaction_id, user_id et merchant_id non vides
- montant strictement positif
- moyen de paiement non vide

Utilisateurs :

- user_id non vide
- age compris entre 18 et 100 ans
- ville non vide

## Rapports de qualite

Deux fichiers CSV sont produits dans le dossier `output/` :

- `quality_summary` : pour chaque source, le nombre de lignes lues, valides, rejetees et le taux de rejet
- `quality_detail` : le nombre de lignes rejetees par motif

## Generation du jar executable

Pour produire un jar autonome avec toutes les dependances :

```
sbt assembly
```

Le jar est genere dans `target/scala-2.12/ecommerce-analytics.jar` et peut etre lance avec spark-submit.

## Parties a venir

- Transformation et enrichissement des donnees
- Analyses metier
- Optimisations Spark
