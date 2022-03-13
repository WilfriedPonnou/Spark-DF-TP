package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
                           .appName("TPdataframe")
                           .master("local[*]")
                           .getOrCreate()
    val codesPostauxDF = spark.read
                              .option("header", "true")
                              .option("delimiter",";")
                              .csv("src/main/resources/codesPostaux.csv")
    
  codesPostauxDF.show()
    
  println("Quel est le schéma du fichier ?")
  codesPostauxDF.printSchema()
    
  println("Affichez le nombre de communes.")
  val question2 = codesPostauxDF.select(countDistinct("Nom_commune").alias("Nb de commune"))
  question2.show(false)
    
  println("Affichez le nombre de communes qui possèdent l’attribut Ligne_5")
  val question3 = codesPostauxDF.filter(col("Ligne_5")=!="").select(countDistinct("nom_commune").alias("Nb de commune avec Ligne_5"))
  question3.show(false)
    
  println("Ajoutez aux données une colonne contenant le numéro de département de la commune. ")
  val question4 = codesPostauxDF.withColumn("departement",col("Code_postal").substr(1,2))
  question4.show(true)

  println("Ecrivez le résultat dans un nouveau fichier CSV")
  val question5 = question4.drop("Ligne_5").drop("Libellé_d_acheminement").drop("coordonnees_gps").sort("Code_postal")
  question5.show()


  question5.write
           .option("header","true")
           .option("delimiter",";")
           .csv("src/main/resources/commune_et_departement")
  
  println("Affichez les communes du département de l’Aisne.")
  val question6= question4.where(question4("departement")==="02")
  question6.show()
  
  println("Quel est le département avec le plus de communes ?")
  val question7= question4.groupBy(col("departement")).agg(countDistinct("Nom_commune").as("Nb_commune")).orderBy(desc("Nb_commune")).show(1)
  println(question7)
  
  spark.close

  }


}
