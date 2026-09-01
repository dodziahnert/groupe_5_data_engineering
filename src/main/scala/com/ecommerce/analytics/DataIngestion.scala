package com.ecommerce.analytics

import org.apache.spark.sql.{Dataset, SparkSession}
import com.ecommerce.models.{Transaction, User, Merchant, Product}
import com.ecommerce.utils.AppConfig

// lecture des fichiers de donnees vers des jeux de donnees types
object DataIngestion {

  // lecture des transactions (csv avec entete)
  def readTransactions(spark: SparkSession): Dataset[Transaction] = {
    import spark.implicits._
    spark.read
      .option("header", "true")           // la premiere ligne contient les noms de colonnes
      .option("inferSchema", "true")      // spark devine les types
      .csv(AppConfig.transactionsPath)
      .as[Transaction]                    // on applique le moule Transaction
  }

  // lecture des utilisateurs (json)
  def readUsers(spark: SparkSession): Dataset[User] = {
    import spark.implicits._
    spark.read
      .json(AppConfig.usersPath)          // le json contient deja sa structure
      .as[User]
  }

  // lecture des marchands (csv avec entete)
  def readMerchants(spark: SparkSession): Dataset[Merchant] = {
    import spark.implicits._
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.merchantsPath)
      .as[Merchant]
  }

  // lecture des produits (parquet)
  def readProducts(spark: SparkSession): Dataset[Product] = {
    import spark.implicits._
    spark.read
      .parquet(AppConfig.productsPath)    // le parquet contient deja son schema
      .as[Product]
  }
}
