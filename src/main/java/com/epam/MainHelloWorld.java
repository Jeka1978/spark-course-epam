package com.epam;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author Evgeny Borisov
 */
public class MainHelloWorld {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hello world").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaRDD<String> linesRdd = sc.textFile("data/taxi/trips.txt");

















//        pairRDD.so
    }
}
