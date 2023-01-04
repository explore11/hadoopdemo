package com.song.hadoopdemo.sparkmllib;

import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 逻辑回归和多项式逻辑回归 java版本
 */
public class LogisticRegressionByJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder().master("local[*]")
                .appName("JavaLogisticRegressionWithElasticNetExample")
                .getOrCreate();

        // 加载训练数据
        Dataset<Row> training = spark.read().format("libsvm").load("data/sample_libsvm_data.txt");

        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // 拟合模型
        LogisticRegressionModel lrModel = lr.fit(training);

        // 打印逻辑回归的系数和截距
        System.out.println("逻辑回归的系数: " + lrModel.coefficients());
        System.out.println("逻辑回归的截距: " + lrModel.intercept());

        //  我们还可以使用多项式族进行二分类 多项式逻辑回归
        LogisticRegression mlr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFamily("multinomial");

        // 拟合模型
        LogisticRegressionModel mlrModel = mlr.fit(training);

        //打印多项式逻辑回归的系数和截距
        System.out.println("多项式逻辑回归的系数: " + lrModel.coefficientMatrix());
        System.out.println("多项式逻辑回归的截距: " + mlrModel.interceptVector());

        spark.stop();
    }
}
