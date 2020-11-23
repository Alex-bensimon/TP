package com.fakir.samples

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SampleProgram {

  // UDF ==> User Defindes Functions
  val transformField = udf((date : String) => {
    val formatDate = new SimpleDateFormat("yyy-MM-dd")
    formatDate.format(new Date(date))

  })

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    // Q1 :
    val df: DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("donnees.csv")
    df.show()
      /*
    // Utilisation de l'UDF
    val transformDateDf = df.withColumn(colName="Order Date", transformField(col(colName="Order Date")))
    transformDateDf.printSchema()
    transformDateDf.show()

    // Q2 : produits avec un prix unitaire > 500€ et plus de 3000 unités vendues
    val new_df = df.filter(col(colName = "Unit Price") > 500  && col(colName = "Units Sold") > 3000)
    val nb = new_df.count()
    println(nb)

    //Question 3 : Faites la somme de tous les produits vendus valant plus de $500 en prix unitaire
    val sum = df.filter(col("Unit Price") >= 100).groupBy("Unit Price").sum("Units Sold")
    sum.show

    //Question 4 : Quel est le prix moyen de tous les produits vendus ? (En groupant par item_type)
    val mean = df.groupBy("Item Type").mean("Unit Price")
    mean.show

    //Question 5 : Créer une nouvelle colonne, "total_revenue" contenant le revenu total des ventes par produit vendu.
    val revenue = df.withColumn("total_revenue", col("Units Sold") * col("Unit Price"))
    //revenue.show
    //Question 6 : Créer une nouvelle colonne, "total_cost", contenant le coût total des ventes par produit vendu.
    val cost = revenue.withColumn("total_cost", col("Units Sold") * col("Unit Cost"))
    //cost.show
    //Question 7 : Créer une nouvelle colonne, "total_profit", contenant le bénéfice réalisé par produit.
    val profit = cost.withColumn("Total Profit", col("total_revenue") - col("total_cost"))

    val discount = profit.withColumn("unit_price_discount", when(col("Units Sold") > 3000, col("Unit Price") * 0.7).otherwise(col("Unit Price") * 0.9))
    //Question8
    val q8 = discount.withColumn("total_revenue", col("Units Sold") * col("unit_price_discount"))
    //Question 9
    val q9 = q8.withColumn("total_profit", when(col("Units Sold") > 3000, col("Units Sold") * (col("Unit Price") - col("Unit Cost"))))

    // Question 10
    val columnsList = df.columns
    val columnsWithoutSpaces : Array[String] = columnsList.map(elem => elem.replaceAll(" ","_"))

    val dfWithRightColumnsNames = df.toDF(columnsWithoutSpaces:_*)
    //dfWithRightColumnsNames.show()
    dfWithRightColumnsNames.write.partitionBy("Sales_Channel").mode(SaveMode.Overwrite).parquet("result")
    val parquetDF = sparkSession.read.parquet("result")
    parquetDF.printSchema()
    parquetDF.show

       */
  }
}