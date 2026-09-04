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

## Module de Transformation et d'Enrichissement des Données

Il permet de consolider les données issues de multiples sources validées, d'appliquer des règles de feature engineering temporel et analytique, et de préparer un jeu de données riche pour l'analyse métier ou la modélisation.

---

### 🚀 Fonctionnalités Principales

#### 1. Consolidation des Données (Joins)
Le pipeline centralise les informations en croisant la table des `Transactions` avec ses dimensions métiers associées :
- **Utilisateurs (`users`)** : Données démographiques (âge, revenus, ville) et comportementales.
- **Produits (`products`)** : Détails des articles (nom, prix, note, stock).
- **Marchands (`merchants`)** : Catégories, taux de commission, localisation.

#### 2. Ingénierie des Caractéristiques Temporelles
Grâce au module `TimeFeatures`, les horodatages bruts (`yyyyMMddHHmmss`) sont parsés de manière robuste pour en extraire des variables catégorielles à haute valeur ajoutée :
- Heure, Jour de la semaine, et Mois.
- Indicateurs booléens (Week-end, Heures ouvrées).
- Période de la journée (Matin, Après-midi, Soirée, Nuit).

#### 3. Calculs Analytiques (Window Functions)
Le module exploite la puissance des fonctions de fenêtrage Spark pour calculer des métriques historiques et glissantes par utilisateur :
- **Historique Client** : Rang de la transaction, nombre total de transactions.
- **Métriques glissantes (7 jours)** : Montant cumulé dépensé, nombre de jours distincts d'activité.
- **Analyse Comportementale** : Écart temporel (en jours) depuis le dernier achat.
- **Profilage du Panier** : Montant moyen historique et pourcentage de déviation du panier actuel par rapport à la moyenne.

#### 4. Détection de Comportements Suspects (Scoring de Fraude)
Le système intègre un moteur de règles pour identifier les transactions potentiellement frauduleuses. Une transaction est marquée `is_suspicious = 1` si elle remplit au moins **2 des conditions suivantes** :
- Le montant dévie de plus de 300% par rapport au panier moyen historique de l'utilisateur.
- La transaction s'effectue de nuit (`Night`).
- L'intervalle avec la transaction précédente est inférieur à 5 minutes (300 secondes).
- Le moyen de paiement utilisé est une cryptomonnaie (`CRYPTO`).

---

#### 📁 Structure des Fichiers

| Fichier | Description |
| :--- | :--- |
| **`DataTransformation.scala`** | Cœur du pipeline de transformation. Gère les jointures, formate les données, applique les *Window functions* et calcule le flag `is_suspicious`. Contient également les fonctions de reporting des anomalies. |
| **`TimeFeatures.scala`** | UDF (User Defined Function) Spark encapsulant la logique d'extraction temporelle. Utilise `java.time` pour des performances optimales et inclut une gestion robuste des erreurs (valeurs nulles ou mal formatées). |
| **`MainApp.scala`** | Point d'entrée de l'application orchestrant l'ingestion, la validation, le profilage qualité, et déclenchant la transformation et le reporting des transactions suspectes. |

---

#### 🛠️ Utilisation et Exécution

L'exécution de la transformation est intégrée dans le job principal du pipeline.

#### Déclenchement de la transformation
```scala
val enriched = DataTransformation.transform(transactions, users, products, merchants)
```

#### Génération du rapport de sécurité
Pour afficher dans la console le nombre de transactions suspectes ainsi que le Top 20 des montants les plus élevés parmi ces transactions :
```scala
DataTransformation.reportSuspicious(enriched)
```

---

### 💡 Notes Techniques
- **Robustesse** : Le typage du fenêtrage glissant sur 7 jours est explicitement casté en `Long` pour garantir la compatibilité stricte avec `rangeBetween`. L'UDF de dates est dotée de *try/catch* pour ne pas interrompre le pipeline en cas d'horodatages corrompus.
- **Optimisation** : Les jointures utilisent des alias explicites pour éviter les conflits de noms de colonnes et les ambiguïtés.