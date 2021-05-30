package com.epam.data_frames;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {

    public static final String LASTNAME = "lastname";
    public static final String AGE = "age";
    public static final String SALARY = "salary";
    public static final String KEYWORDS = "keywords";
    public static final String KEYWORD = "keyword";

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);


        SparkSession spark = SparkSession.builder().appName("abc").master("local[*]").getOrCreate();




        Dataset<Row> dataFrame = spark.read().json("data/linkedIn/*");
        dataFrame.show();


        dataFrame.printSchema();


        StructField[] fields = dataFrame.schema().fields();
        for (StructField field : fields) {
            System.out.println(field.dataType());
            System.out.println(field.name());
            System.out.println(field.nullable());
        }


        dataFrame.persist();
        dataFrame = dataFrame.withColumn(LASTNAME, upper(col("name")));
        dataFrame.withColumn("age to live remaining", col(AGE).minus(120).multiply(-1)).show();

        dataFrame.show();



        dataFrame.registerTempTable("linkedin");

        spark.sql("select * from linkedin where age > 40").show();
        dataFrame.where(col("age").$greater(40)).show();


        Dataset<Row> salaryDf = dataFrame.withColumn(SALARY, col("age").multiply(10).multiply(size(col(KEYWORDS))));
//        salaryDf.persist();
        salaryDf.show();



        Row row = dataFrame.withColumn(KEYWORD, explode(col(KEYWORDS))).select(KEYWORD)
                .groupBy(col(KEYWORD)).agg(count(KEYWORD).as("amount")).sort(col("amount").desc()).first();

        String mostPopular = row.getAs(KEYWORD);
        long amount = row.getAs("amount");

        System.out.println(amount+10);
        System.out.println(mostPopular);

        salaryDf.filter(col(SALARY).leq(1200)
                .and(array_contains(col(KEYWORDS),mostPopular)))
                .show();

        salaryDf.registerTempTable("employees");

        String sqlText = "select * from employees where salary < 1200";
        Dataset<Row> dataFrame1 = spark.sql(sqlText);

        salaryDf=null;
        printSomething(spark);

    }

    private static void printSomething(SparkSession spark) {
        spark.sql("select * from employees where salary < 1200").show();
    }
}
