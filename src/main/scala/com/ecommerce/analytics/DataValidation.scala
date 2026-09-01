package com.ecommerce.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.ecommerce.models.ValidationResult

// validation des donnees : separe les lignes valides des lignes rejetees
object DataValidation {

  // colonne technique qui contient le motif de rejet
  private val reasonCol = "rejection_reason"

  // ------------------------------------------------------------
  // validation des transactions
  // regles : ids non vides, montant > 0, moyen de paiement non vide
  // ------------------------------------------------------------
  def validateTransactions(df: DataFrame): ValidationResult = {

    // on construit le motif de rejet en empilant les problemes trouves
    val withReason = df.withColumn(reasonCol,
      concat_ws("; ",
        when(col("transaction_id").isNull || col("transaction_id") === "", "transaction_id manquant"),
        when(col("user_id").isNull || col("user_id") === "", "user_id manquant"),
        when(col("merchant_id").isNull || col("merchant_id") === "", "merchant_id manquant"),
        when(col("amount").isNull || col("amount") <= 0, "montant invalide"),
        when(col("payment_method").isNull || col("payment_method") === "", "paiement manquant")
      )
    )

    split(withReason)
  }

  // ------------------------------------------------------------
  // validation des utilisateurs
  // regles : user_id non vide, age entre 18 et 100, ville non vide
  // ------------------------------------------------------------
  def validateUsers(df: DataFrame): ValidationResult = {

    val withReason = df.withColumn(reasonCol,
      concat_ws("; ",
        when(col("user_id").isNull || col("user_id") === "", "user_id manquant"),
        when(col("age").isNull || col("age") < 18 || col("age") > 100, "age invalide"),
        when(col("city").isNull || col("city") === "", "ville manquante")
      )
    )

    split(withReason)
  }

  // ------------------------------------------------------------
  // coupe le jeu de donnees en deux selon la presence d'un motif
  // ------------------------------------------------------------
  private def split(withReason: DataFrame): ValidationResult = {

    // lignes valides : aucun motif de rejet
    val valid = withReason
      .filter(col(reasonCol) === "")
      .drop(reasonCol)

    // lignes rejetees : au moins un motif
    val rejected = withReason
      .filter(col(reasonCol) =!= "")

    ValidationResult(valid, rejected)
  }
}
