package com.epam.data_frames;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {

    public static final String LASTNAME = "lastname";
    public static final String AGE = "age";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("linkedIn").setMaster("local[*]");
        SparkContext sc = new SparkContext(sparkConf);
        SQLContext spark = new SQLContext(sc);

        DataFrame dataFrame = spark.read().json("data/linkedIn/*");
        dataFrame.show();

        dataFrame.printSchema();

        dataFrame.persist();
        dataFrame = dataFrame.withColumn(LASTNAME, upper(col("name")));
        dataFrame.withColumn("age to live remaining", col(AGE).cast(DataTypes.IntegerType).minus(120).multiply(-1)).show();

        dataFrame.show();

        dataFrame.withColumn("keyword",explode(col("keywords"))).drop("name").drop("age").show();


        dataFrame.registerTempTable("linkedin");

        spark.sql("select * from linkedin where age > 40").show();
        dataFrame.where(col("age").$greater(40)).show();
    }
}
