package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

// lit le fichier application.conf et expose les valeurs
object AppConfig {

  // charge automatiquement application.conf
  private val config: Config = ConfigFactory.load()

  // parametres de l'application
  val appName: String = config.getString("app.name")
  val master: String = config.getString("app.master")

  // chemins des fichiers de donnees
  val transactionsPath: String = config.getString("data.transactions")
  val usersPath: String = config.getString("data.users")
  val merchantsPath: String = config.getString("data.merchants")
  val productsPath: String = config.getString("data.products")

  // dossier de sortie
  val outputPath: String = config.getString("output.path")
}
