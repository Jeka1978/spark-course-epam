package com.epam

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Evgeny Borisov
 */
object MainHelloWorldScala {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Hello spark from scala").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[String] = sc.textFile("data/hello.txt")
    val rdd2: RDD[Person] = rdd1.map(line => Person(line))
    val rdd3: RDD[(String, Int)] = rdd2.map(p => Tuple2(p.name, p.name.length))
    val rddTotal: RDD[(String, Int)] = rdd3.reduceByKey(_ + _)
    rdd1.persist(StorageLevel.MEMORY_AND_DISK)

    val x = rdd2.count()
    val l = rdd3.count()
  }

}
