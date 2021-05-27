package com.epam.udf_examples;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @author Evgeny Borisov
 */

public class CalcNicknameUdf implements UDF1<String,String> {

    @Override
    public String call(String s) throws Exception {
        String[] strings = s.split(" ");
        return strings[0].length() > strings[1].length() ? strings[0] : strings[1];
    }
}
