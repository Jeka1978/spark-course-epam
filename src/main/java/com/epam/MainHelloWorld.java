package com.epam;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Evgeny Borisov
 */
public class MainHelloWorld {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hello world").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = sc.textFile("data/hello.txt");
        rdd.filter(line->line.startsWith("j")).collect().forEach(System.out::println);


    }
}
