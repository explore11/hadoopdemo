package com.song.hadoopdemo.sparkmllib

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 线性回归算法 scala 版本
 *
 */
object LinearRegressionByScala {
  def main(args: Array[String]): Unit = {
    // 构建Spark对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("LinearRegressionWithSGD")
    val sc = new SparkContext(conf)
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 加载训练数据
    val training = spark.read.format("libsvm").load("data/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 拟合模型
    val lrModel = lr.fit(training)

    // 打印线性回归的系数和截距
    println(s"线性回归的系数: ${lrModel.coefficients} 线性回归的截距: ${lrModel.intercept}")

    // 在训练总结模型上打印出一些指标
    val trainingSummary = lrModel.summary
    //迭代次数
    println(s"迭代次数: ${trainingSummary.totalIterations}")
    //每次迭代的目标函数
    println(s"每次迭代的目标函数: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //残余
    println(s"残余: ${trainingSummary.residuals.show()}")
    //均方根误差
    println(s"均方根误差: ${trainingSummary.rootMeanSquaredError}")
    //决定系数
    println(s"决定系数: ${trainingSummary.r2}")

    //关闭
    sc.stop()
  }
}
