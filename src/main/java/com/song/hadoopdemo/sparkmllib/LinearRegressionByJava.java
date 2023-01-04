package com.song.hadoopdemo.sparkmllib;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * java 线性回归算法
 */
public class LinearRegressionByJava {
    public static void main(String[] args) {
        // 构建Spark对象
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("LinearRegressionWithSGD");
        SparkContext sc = new SparkContext(conf);
        //创建 SparkSession 对象
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 加载训练数据
        Dataset<Row> training = spark.read().format("libsvm").load("data/sample_linear_regression_data.txt");

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // 拟合模型
        LinearRegressionModel lrModel = lr.fit(training);

        // 打印线性回归的系数和截距
        System.out.println("线性回归的系数: " + lrModel.coefficients() + " 线性回归的截距: " + lrModel.intercept());

        // 在线性回归训练总结模型上打印出一些指标
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("迭代次数: " + trainingSummary.totalIterations());
        System.out.println("每次迭代的目标函数: " + Vectors.dense(trainingSummary.objectiveHistory()));
        // 残余（标签 - 预测值）
        Dataset<Row> residuals = trainingSummary.residuals();
        residuals.show(100);
        System.out.println("均方根误差: " + trainingSummary.rootMeanSquaredError());
        System.out.println("决定系数: " + trainingSummary.r2());
    }
}
