package com.song.hadoopdemo.mllib;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.ChiSquareTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;

/**
 * 假设检验测试
 */
public class HypothesisTest {
    public static void main(String[] args) {
        //创建 SparkSession 对象
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Correlation").getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(0.0, Vectors.dense(0.5, 10.0)),
                RowFactory.create(0.0, Vectors.dense(1.5, 20.0)),
                RowFactory.create(1.0, Vectors.dense(1.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 30.0)),
                RowFactory.create(0.0, Vectors.dense(3.5, 40.0)),
                RowFactory.create(1.0, Vectors.dense(3.5, 40.0))
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);
        Row r = ChiSquareTest.test(df, "features", "label").head();
        System.out.println("pValues: " + r.get(0).toString());
        System.out.println("degreesOfFreedom: " + r.getList(1).toString());
        System.out.println("statistics: " + r.get(2).toString());
    }
}
