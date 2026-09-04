package com.ecommerce.analytics

import com.ecommerce.utils.{SparkSessionBuilder, AppConfig}
import com.ecommerce.models.ValidationResult

// point d'entree du programme — orchestration complete (Partie 6)
object MainApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionBuilder.build()
    spark.sparkContext.setLogLevel("WARN")

    // Partie 5.2 — nombre de partitions de shuffle pilote par config
    SparkOptimizations.configureShufflePartitions(spark)

    try {
      // ------------------------------------------------------ lecture
      val transactions = DataIngestion.readTransactions(spark).toDF()
      val users = DataIngestion.readUsers(spark).toDF()
      val merchants = DataIngestion.readMerchants(spark).toDF()
      val products = DataIngestion.readProducts(spark).toDF()

      // ---------------------------------------------------- validation
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

      // ------------------------------------------------- transformation
      // Utilisation des donnees VALIDEES (txResult.valid / userResult.valid)
      // plutot que des donnees brutes, pour ne pas polluer les analyses
      // avec des lignes rejetees (ages impossibles, montants negatifs...).
      val enriched = SparkOptimizations.cacheIfEnabled(
        DataTransformation.transform(txResult.valid, userResult.valid, products, merchants)
      )
      DataTransformation.reportSuspicious(enriched)

      // ------------------------------------------------------ analytics
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

      SparkOptimizations.unpersistSafe(enriched)

    } catch {
      case e: Exception =>
        println(s"[ERREUR] Echec du pipeline : ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
