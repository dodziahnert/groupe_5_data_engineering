package com.ecommerce.analytics

import com.ecommerce.utils.SparkSessionBuilder

// point d'entree du programme (version de test)
object MainApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSessionBuilder.build()
    spark.sparkContext.setLogLevel("WARN")

    // lecture
    val transactions = DataIngestion.readTransactions(spark).toDF()
    val users = DataIngestion.readUsers(spark).toDF()

    // validation des transactions
    val txResult = DataValidation.validateTransactions(transactions)
    println(">>> Transactions valides : " + txResult.valid.count())
    println(">>> Transactions rejetees : " + txResult.rejected.count())
    txResult.rejected.select("transaction_id", "amount", "rejection_reason").show(5, false)

    // validation des utilisateurs
    val userResult = DataValidation.validateUsers(users)
    println(">>> Utilisateurs valides : " + userResult.valid.count())
    println(">>> Utilisateurs rejetes : " + userResult.rejected.count())
    userResult.rejected.select("user_id", "age", "city", "rejection_reason").show(5, false)

    spark.stop()
  }
}
