package com.ecommerce.analytics

import com.ecommerce.utils.{SparkSessionBuilder, AppConfig}
import com.ecommerce.models.ValidationResult

// point d'entree du programme (version de test)
object MainApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionBuilder.build()
    spark.sparkContext.setLogLevel("WARN")

    // lecture
    val transactions = DataIngestion.readTransactions(spark).toDF()
    val users = DataIngestion.readUsers(spark).toDF()
    val merchants = DataIngestion.readMerchants(spark).toDF()
    val products = DataIngestion.readProducts(spark).toDF()

    // validation
    val txResult = DataValidation.validateTransactions(transactions)
    val userResult = DataValidation.validateUsers(users)

    // on regroupe les resultats par source
    val results = Map(
      "transactions" -> txResult,
      "users" -> userResult
    )

    // rapport de qualite : recap global
    val summary = DataQualityReport.buildSummary(spark, results)
    println(">>> Recap qualite des donnees")
    summary.show(false)

    // rapport de qualite : detail par motif
    val detail = DataQualityReport.buildDetail(spark, results)
    println(">>> Detail par motif de rejet")
    detail.show(false)

    // sauvegarde des deux rapports en csv
    DataQualityReport.writeSingleCsv(summary, AppConfig.outputPath + "/quality_summary")
    DataQualityReport.writeSingleCsv(detail, AppConfig.outputPath + "/quality_detail")
    println(">>> Rapports sauvegardes dans le dossier " + AppConfig.outputPath)

    val enriched = DataTransformation.transform(transactions, users, products, merchants)
    DataTransformation.reportSuspicious(enriched)

    spark.stop()
  }
}
