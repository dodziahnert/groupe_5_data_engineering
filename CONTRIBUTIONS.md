# Contributions et choix techniques

Ce document trace la contribution de chaque membre et justifie les principaux choix techniques.

## Ingestion, validation et qualite (Membre A - Mamoudou BAH)

### Choix de l'environnement
Le couple Scala 2.12.18 et Spark 3.5.1 a ete retenu pour sa stabilite et sa large documentation, Spark etant historiquement construit sur Scala 2.12. Java 17 est impose par la compatibilite de Spark 3.5.1, qui ne fonctionne pas correctement avec Java 21 ; un fichier .jvmopts avec des options d'ouverture de modules a ete ajoute pour permettre a Spark de demarrer sur les versions recentes de Java.

### Configuration et structure
Les dependances Spark sont declarees en "provided" : necessaires a la compilation mais fournies par l'environnement a l'execution, ce qui allege le jar final. Les chemins des fichiers et les parametres sont externalises dans application.conf, plutot qu'ecrits en dur dans le code, afin de centraliser toute modification en un seul endroit.

### Modelisation
Les case classes reprennent les noms des colonnes sources pour permettre a Spark d'associer automatiquement les donnees aux champs par leur nom. Les dates sont conservees en texte au stade de l'ingestion, la transformation etant laissee a l'etape suivante. Le champ age est type en Long car Spark lit les entiers d'un fichier JSON comme des entiers larges.

### Validation
Une ligne qui ne respecte pas toutes les regles est rejetee, avec conservation du motif du rejet. Les regles retenues : pour les transactions, identifiants non vides, montant strictement positif et moyen de paiement renseigne ; pour les utilisateurs, identifiant non vide, age entre 18 et 100 ans et ville renseignee. Les motifs se cumulent lorsqu'une ligne presente plusieurs problemes. Le choix de separer les lignes valides et rejetees, plutot que de simplement filtrer, permet de produire un rapport de qualite exploitable.

### Rapport de qualite
Deux rapports CSV sont produits : un recapitulatif global (lignes lues, valides, rejetees, taux de rejet) et un detail par motif. L'ecriture utilise coalesce(1) pour obtenir un fichier unique, choix acceptable ici vu le faible volume du rapport.

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

Chaque partie a ete relue par un autre membre avant fusion dans la branche principale, via les pull requests du depot.
