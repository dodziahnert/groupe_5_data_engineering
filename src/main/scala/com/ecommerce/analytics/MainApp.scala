package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.ecommerce.utils.{SparkSessionBuilder, AppConfig}

// point d'entree du programme
// accepte un argument : ingestion | transformation | analytics | all
object MainApp {

  // format d'affichage de l'heure
  private val timeFmt = DateTimeFormatter.ofPattern("HH:mm:ss")

  def main(args: Array[String]): Unit = {

    // etape demandee (all par defaut si aucun argument)
    val stage = if (args.nonEmpty) args(0).toLowerCase else "all"
    val accepted = Set("ingestion", "transformation", "analytics", "all")

    // argument inconnu : on affiche l'aide et on s'arrete sans exception
    if (!accepted.contains(stage)) {
      printHelp(stage)
      return
    }

    val spark = SparkSessionBuilder.build()
    spark.sparkContext.setLogLevel("WARN")
    SparkOptimizations.configureShufflePartitions(spark)

    try {
      stage match {

        case "ingestion" =>
          timed("Ingestion") { stageIngestion(spark) }

        case "transformation" =>
          val (txValid, userValid, products, merchants) =
            timed("Ingestion") { stageIngestion(spark) }
          val enriched =
            timed("Transformation") { stageTransformation(txValid, userValid, products, merchants) }
          SparkOptimizations.unpersistSafe(enriched)

        case "analytics" | "all" =>
          val (txValid, userValid, products, merchants) =
            timed("Ingestion") { stageIngestion(spark) }
          val enriched =
            timed("Transformation") { stageTransformation(txValid, userValid, products, merchants) }
          timed("Analytics") { stageAnalytics(spark, enriched) }
          SparkOptimizations.unpersistSafe(enriched)

        case _ =>
          // securite : ne devrait jamais arriver grace au controle plus haut
          printHelp(stage)
      }
    } catch {
      case e: Exception =>
        println(s"[ERREUR] Echec du pipeline : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  // ------------------------------------------------------------
  // mesure et journalise le debut, la fin et la duree d'une etape
  // ------------------------------------------------------------
  private def timed[T](label: String)(block: => T): T = {
    val start = LocalDateTime.now()
    println(s"[$label] debut : ${start.format(timeFmt)}")
    val t0 = System.nanoTime()

    val result = block

    val dureeSec = (System.nanoTime() - t0) / 1e9
    val end = LocalDateTime.now()
    println(s"[$label] fin   : ${end.format(timeFmt)}")
    println(f"[$label] duree : $dureeSec%.2f s")
    result
  }

  // ------------------------------------------------------------
  // etape ingestion : lecture, validation et rapport de qualite
  // renvoie les donnees validees et les tables de reference
  // ------------------------------------------------------------
  private def stageIngestion(spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame) = {

    val transactions = DataIngestion.readTransactions(spark).toDF()
    val users = DataIngestion.readUsers(spark).toDF()
    val merchants = DataIngestion.readMerchants(spark).toDF()
    val products = DataIngestion.readProducts(spark).toDF()

    val txResult = DataValidation.validateTransactions(transactions)
    val userResult = DataValidation.validateUsers(users)

    val results = Map(
      "transactions" -> txResult,
      "users" -> userResult
    )

    val summary = DataQualityReport.buildSummary(spark, results)
    println(">>> Recap qualite des donnees")
    summary.show(false)

    val detail = DataQualityReport.buildDetail(spark, results)
    println(">>> Detail par motif de rejet")
    detail.show(false)

    DataQualityReport.writeSingleCsv(summary, AppConfig.outputPath + "/quality_summary")
    DataQualityReport.writeSingleCsv(detail, AppConfig.outputPath + "/quality_detail")
    println(">>> Rapports qualite sauvegardes dans " + AppConfig.outputPath)

    (txResult.valid, userResult.valid, products, merchants)
  }

  // ------------------------------------------------------------
  // etape transformation : enrichissement des donnees validees
  // ------------------------------------------------------------
  private def stageTransformation(txValid: DataFrame, userValid: DataFrame,
                                  products: DataFrame, merchants: DataFrame): DataFrame = {

    val enriched = SparkOptimizations.cacheIfEnabled(
      DataTransformation.transform(txValid, userValid, products, merchants)
    )
    DataTransformation.reportSuspicious(enriched)
    enriched
  }

  // ------------------------------------------------------------
  // etape analytics : analyses metier a partir des donnees enrichies
  // ------------------------------------------------------------
  private def stageAnalytics(spark: SparkSession, enriched: DataFrame): Unit = {

    val merchantKpi = Analytics.merchantReport(enriched)
    println(">>> KPI par marchand")
    merchantKpi.show(20, truncate = false)
    DataQualityReport.writeSingleCsv(merchantKpi, AppConfig.outputPath + "/merchant_report")

    val ageDistribution = Analytics.merchantAgeDistribution(enriched)
    DataQualityReport.writeSingleCsv(ageDistribution, AppConfig.outputPath + "/merchant_age_distribution")

    val retention = Analytics.cohortRetentionMatrix(enriched)
    println(">>> Matrice de retention par cohorte")
    retention.show(50, truncate = false)
    DataQualityReport.writeSingleCsv(retention, AppConfig.outputPath + "/cohort_retention")

    val bestCohort = Analytics.bestCohortAt3Months(retention)
    println(">>> Meilleure cohorte a 3 mois")
    bestCohort.show(false)

    val fraud = Analytics.fraudByMerchant(enriched)
    println(">>> Transactions suspectes par marchand")
    fraud.show(20, truncate = false)
    DataQualityReport.writeSingleCsv(fraud, AppConfig.outputPath + "/fraud_by_merchant")
  }

  // ------------------------------------------------------------
  // message d'aide affiche pour un argument inconnu
  // ------------------------------------------------------------
  private def printHelp(unknown: String): Unit = {
    println(s"[AIDE] argument inconnu : '$unknown'")
    println("Valeurs acceptees :")
    println("  ingestion       lecture, validation et rapport de qualite")
    println("  transformation  ingestion puis enrichissement des donnees")
    println("  analytics       ingestion, transformation puis analyses metier")
    println("  all             tout le pipeline (valeur par defaut)")
    println("Exemple : spark-submit --class com.ecommerce.analytics.MainApp app.jar transformation")
  }
}
