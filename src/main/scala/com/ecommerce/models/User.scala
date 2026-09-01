package com.ecommerce.models

// moule d'un utilisateur (fichier users.json)
case class User(
  user_id: String,                     // identifiant du client
  age: Long,                           // age
  annual_income: Double,               // revenu annuel
  city: String,                        // ville
  customer_segment: String,            // segment (Budget, Standard, Premium, VIP)
  preferred_categories: Seq[String],   // categories preferees (1 a 4)
  registration_date: String            // date d'inscription brute (aaaammjj)
)
