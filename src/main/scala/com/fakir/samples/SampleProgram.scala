package com.fakir.samples

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.longToLiteral
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.Int.{int2double, int2float}
import scala.math.BigDecimal.double2bigDecimal

object SampleProgram {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

  // EXERCICE 1

    // Q1 :
    val rdd:RDD[String]= sparkSession.sparkContext.textFile("films.csv")

    // Q2 :
    val DiCaprio_count = rdd.filter(elem => elem.contains("Di Caprio")).count()
    DiCaprio_count.foreach(println)

    // Q3 :
    val DiCaprio = rdd.filter(elem => elem.contains("Di Caprio"))
    val rate_count = DiCaprio.map(item => (item.split(";")(2).toDouble))
    val count_dicaprio = rate_count.sum
    println("Moyenne : " + count_dicaprio/DiCaprio.count())

    // Q4 :
    val new_count = rdd.map(item => (item.split(";")(1).toDouble))
    val tot = new_count.sum
    val views_count = DiCaprio.map(item => (item.split(";")(1).toDouble))
    val count_dicaprio_views = views_count.sum
    val purcent_dicaprio = count_dicaprio_views / tot * 100
    println(purcent_dicaprio)

    // Q5 :




  /*
    // EXERCICE 2 :

    // Q1 :
    val df : DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("films.csv")

    // Q2 :
    val df_renamed = df.withColumnRenamed("_c0","nom_film")
      .withColumnRenamed("_c1","nombre_vues").withColumnRenamed("_c2","note_film")
      .withColumnRenamed("_c3","acteur_principal")
    df_renamed.printSchema()

    // Q3 :
    val new_df = df_renamed.filter(col(colName = "acteur_principal")==="Di Caprio")
    val nb = new_df.count()
    println(nb)

    val mean = new_df.groupBy("acteur_principal").mean("note_film")
    mean.show

    val sum = new_df.groupBy("acteur_principal").sum("nombre_vues")
    sum.show

    val sum_tot = df_renamed.select(col("nombre_vues")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    val sum_value = sum.first().getLong(1)

    val purcent = (sum_value.toDouble / sum_tot.toDouble)*100
    println(purcent)

    val rate_mean = df_renamed.groupBy("acteur_principal").mean("note_film")
    rate_mean.show

    val view_mean = df_renamed.groupBy("acteur_principal").mean("nombre_vues")
    view_mean.show

    val new_column = df_renamed.withColumn("pourcentage_de_vues", (col("nombre_vues") / sum_tot)*100)
    new_column.show()
  */

    // Q1 :
    //val df: DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", true).csv("films.csv")
    //df.show()
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
  /*
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()


    val rdd = sparkSession.sparkContext.textFile("{data/dataPop.csv}")
    val filter_rdd = rdd.filter(elem => !elem.startsWith("H") || elem.split(";")(1).toDouble > 2)
    val majRDD = filter_rdd.map((elem: String) => elem.split(";")(2))


    val counts = rdd.map(item => (item.split(";")(2).toDouble, (1.0, item.split(";")(1).toDouble)))
    val countSums = counts.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    val keyMeans = countSums.mapValues(avgCount => avgCount._2 / avgCount._1)
    keyMeans.foreach(println)
  }
  */
}