package com.ecommerce.models

// moule d'un marchand (fichier merchants.csv)
case class Merchant(
  merchant_id: String,        // identifiant du marchand
  name: String,               // nom du marchand
  category: String,           // categorie
  region: String,             // region
  commission_rate: Double,    // taux de commission
  establishment_date: String  // date de creation brute (aaaammjj)
)
