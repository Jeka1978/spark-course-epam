package com.epam.data_frames

import com.epam.Person
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Evgeny Borisov
 */
object ScalaSolutionForLinkedIn {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("linkedIn").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val spark = new SQLContext(sc)
    val dataFrame = spark.read.json("data/linkedIn/*")
    dataFrame.schema.fields.foreach(println(_))



    val salaryDf = dataFrame
      .withColumn("salary", col("age") * 10 * size(col("keywords")))


    salaryDf.unionAll(dataFrame)

    salaryDf
      .withColumn("keyword", explode(col("keywords")))
      .select("keyword")
      .groupBy("keyword") //todo finish by yourself


  }

}
