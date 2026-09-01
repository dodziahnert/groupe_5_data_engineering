package com.ecommerce.models

// moule d'une transaction (fichier transactions.csv)
case class Transaction(
  transaction_id: String,   // identifiant de la transaction
  user_id: String,          // identifiant du client
  product_id: String,       // identifiant du produit
  merchant_id: String,      // identifiant du marchand
  amount: Double,           // montant paye
  timestamp: String,        // date et heure brute (aaaammjjhhmmss)
  location: String,         // ville de l'achat
  payment_method: String,   // moyen de paiement
  category: String          // categorie du produit
)
