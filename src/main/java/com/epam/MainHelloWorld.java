package com.epam;

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

        JavaRDD<String> lines = sc.textFile("data/hello.txt");

        JavaPairRDD<String, Integer> pairRDD = lines.map(Person::new).mapToPair(person -> Tuple2.apply(person.name(), person.name().length()));

        JavaPairRDD<String, Integer> byKey = pairRDD.reduceByKey(Integer::sum);

//        pairRDD.so
    }
}
