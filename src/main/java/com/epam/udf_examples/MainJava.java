package com.epam.udf_examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class MainJava {


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkConf sparkConf = new SparkConf().setAppName("linkedIn").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext spark = new SQLContext(sc);

        DataFrame dataFrame = spark.read().json("data/linkedIn/*");

        spark.udf().register(CalcNicknameUdf.class.getName(),new CalcNicknameUdf(), DataTypes.StringType);

        dataFrame.withColumn("nickname", callUDF(CalcNicknameUdf.class.getName(),col("name"))).show();

    }
}











