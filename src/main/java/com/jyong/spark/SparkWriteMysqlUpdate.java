package com.jyong.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

/**
 * @author jyong
 * @date 2022年09月04日 15:03
 * @desc:
 */
public class SparkWriteMysqlUpdate {



    public static void main(String[] args) {

        //创建session
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("wirte_mysql").getOrCreate();

        //创建dataframe schma
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));


        //创建数据rows
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, "a"));
        rows.add(RowFactory.create(2, "b"));
        rows.add(RowFactory.create(3, "c"));
        rows.add(RowFactory.create(4, "c"));

        //创建dataframe
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rows, DataTypes.createStructType(structFields));

        dataFrame.show();

        //写入mysql

        dataFrame.write()
//                .mode(SaveMode.ReplaceInto)
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306")
                .option("dbtable", "test.test")
                .option("user", "root")
                .option("password", "123456")
                .save();


        sparkSession.close();


    }


}
