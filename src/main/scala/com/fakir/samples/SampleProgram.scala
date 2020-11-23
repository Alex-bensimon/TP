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

    /*
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

  */



    // EXERCICE 2 :

    // Q1 :
    val df : DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("films.csv")

    // Q2 :
    val df_renamed = df.withColumnRenamed("_c0","nom_film")
      .withColumnRenamed("_c1","nombre_vues").withColumnRenamed("_c2","note_film")
      .withColumnRenamed("_c3","acteur_principal")
    df_renamed.printSchema()

    // Q3 :
    // Q3.2
    val new_df = df_renamed.filter(col(colName = "acteur_principal")==="Di Caprio")
    val nb = new_df.count()
    println(nb)

    // Q3.3
    val mean = new_df.groupBy("acteur_principal").mean("note_film")
    mean.show

    // Q3.4
    val sum = new_df.groupBy("acteur_principal").sum("nombre_vues")
    sum.show

    val sum_tot = df_renamed.select(col("nombre_vues")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
    val sum_value = sum.first().getLong(1)

    val purcent = (sum_value.toDouble / sum_tot.toDouble)*100
    println(purcent)

    val rate_mean = df_renamed.groupBy("acteur_principal").mean("note_film")
    rate_mean.show

    // Q3.5
    val view_mean = df_renamed.groupBy("acteur_principal").mean("nombre_vues")
    view_mean.show

    // Q4 :
    val new_column = df_renamed.withColumn("pourcentage_de_vues", (col("nombre_vues") / sum_tot)*100)
    new_column.show()




  }
}