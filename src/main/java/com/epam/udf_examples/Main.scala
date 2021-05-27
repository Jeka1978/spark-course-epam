package com.epam.udf_examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, functions}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * @author Evgeny Borisov
 */
object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sc = new SparkContext(new SparkConf().setAppName("taxi").setMaster("local[*]"))
    val spark = new SQLContext(sc)

    val rddOfRow = sc.textFile("data/taxi/trips.txt")
      .map(_.split(" "))
      .map(arr =>Row(arr(0), arr(1), arr(2).toInt))


    val schema: StructType = DataTypes.createStructType(Array(
      DataTypes.createStructField("id", DataTypes.StringType, true),
      DataTypes.createStructField("city name", DataTypes.StringType, true),
      DataTypes.createStructField("distance", DataTypes.IntegerType, true))
    )

    val dataFrame = spark.createDataFrame(rddOfRow, schema)


    val countCityWordsUdf = udf{ (city:String,km:Int,l:String)=>city.split(" ").length*km* l.asInstanceOf[Person].name.length}

    dataFrame.withColumn("city words count",countCityWordsUdf(col("city name"),col("distance"),lit(Person("JOHN")))).show()
  }
}
