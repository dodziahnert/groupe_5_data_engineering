package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

// fabrique la session spark (le moteur de traitement)
object SparkSessionBuilder {

  def build(): SparkSession = {
    SparkSession.builder()
      .appName(AppConfig.appName)   // nom de l'application (depuis la config)
      .master(AppConfig.master)     // mode d'execution (depuis la config)
      .getOrCreate()                // cree la session ou reutilise l'existante
  }
}
