package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models.ValidationResult

// genere le rapport de qualite des donnees
object DataQualityReport {

  // ------------------------------------------------------------
  // construit le recap global : une ligne par source
  // colonnes : source, lues, valides, rejetees, taux_rejet
  // ------------------------------------------------------------
  def buildSummary(spark: SparkSession,
                   results: Map[String, ValidationResult]): DataFrame = {
    import spark.implicits._

    // pour chaque source, on calcule les compteurs
    val rows = results.toSeq.map { case (source, result) =>
      val valid = result.valid.count()
      val rejected = result.rejected.count()
      val total = valid + rejected
      // taux de rejet en pourcentage, arrondi a 2 decimales
      val rate = if (total == 0) 0.0 else (rejected.toDouble / total) * 100
      (source, total, valid, rejected, Math.round(rate * 100.0) / 100.0)
    }

    rows.toDF("source", "lues", "valides", "rejetees", "taux_rejet")
  }

  // ------------------------------------------------------------
  // construit le detail par motif de rejet
  // colonnes : source, motif, nombre
  // ------------------------------------------------------------
  def buildDetail(spark: SparkSession,
                  results: Map[String, ValidationResult]): DataFrame = {
    import spark.implicits._

    // pour chaque source, on compte les lignes par motif
    val details = results.toSeq.map { case (source, result) =>
      result.rejected
        .groupBy("rejection_reason")
        .count()
        .withColumn("source", lit(source))
        .select("source", "rejection_reason", "count")
    }

    // on empile tous les tableaux en un seul
    details.reduce(_ union _)
      .withColumnRenamed("rejection_reason", "motif")
      .withColumnRenamed("count", "nombre")
      .orderBy("source", "motif")
  }

  // ------------------------------------------------------------
  // ecrit un dataframe en un seul fichier csv propre
  // ------------------------------------------------------------
  def writeSingleCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)                 // regroupe en une seule partition
      .write
      .option("header", "true")    // ecrit la ligne d'entete
      .mode("overwrite")           // remplace si le fichier existe deja
      .csv(path)
  }
}
