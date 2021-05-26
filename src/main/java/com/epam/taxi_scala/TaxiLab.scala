package com.epam.taxi_scala

import com.epam.taxi_scala.model.Trip
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
 * @author Evgeny Borisov
 */
object TaxiLab {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    Logger.getLogger("org.apache").setLevel(Level.WARN)
    val tripsRdd = sc.textFile("data/taxi/trips.txt")
      .map(line => line.split(" "))
      .map(arr => Trip(id = arr(0), city = arr(1).toLowerCase, distance = arr(2).toInt))

    tripsRdd.persist(StorageLevel.MEMORY_AND_DISK)

    val bostonRdd = tripsRdd.filter(_.city == "boston").persist()

    val longTripsToBoston = bostonRdd.filter(_.distance > 10).count()
    println(s"long trips to boston is: ${longTripsToBoston}")

    val totalKmToBoston = bostonRdd.map(_.distance).sum()
    println(s"total km to boston $totalKmToBoston")

    val driversRdd = sc.textFile("data/taxi/drivers.txt")
      .map(line => line.split(", "))
      .map(arr => (arr(0), arr(1)))
    /*
        tripsRdd
          .map(trip => (trip.id, trip.distance))
          .reduceByKey(_ + _)
          .join(driversRdd)
          .sortBy(_._2._1,ascending = false)*/

    tripsRdd
      .map(trip => (trip.id, trip.distance))
      .reduceByKey(_ + _)
      .join(driversRdd)
      .map(_._2)
      .sortByKey(ascending = false)
      .take(3)
      .foreach(println(_))


    var short: Accumulator[Int] =sc.accumulator(0)
    var long = sc.accumulator(0)

    tripsRdd.foreach(trip => {
      if (trip.distance >= 10) long.add(1)
      if (trip.distance < 10) short += 1
    })

    println(s"long: ${long.value}")
    println(s"short: ${short.value}")


  }

}








