// nom du projet
name := "EcommerceAnalytics"

// version du projet
version := "1.0"

// version de scala
scalaVersion := "2.12.18"

// version de spark
val sparkVersion = "3.5.1"

// liste des librairies utilisees
libraryDependencies ++= Seq(
  // spark de base
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  // spark sql pour les dataframes
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  // lecture du fichier application.conf
  "com.typesafe" % "config" % "1.4.3"
)

// nom du jar final
assembly / assemblyJarName := "ecommerce-analytics.jar"

// classe principale a lancer
assembly / mainClass := Some("com.ecommerce.analytics.MainApp")

// regle quand deux librairies ont un fichier du meme nom
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// permet de lancer le projet en local avec sbt run
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

run / fork := true
