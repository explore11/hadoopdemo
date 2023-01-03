package com.song.hadoopdemo.sparkmllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.mllib.util.LogisticRegressionDataGenerator

object rdd_test02 {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd_test_2.2.1")
    val sc = new SparkContext(conf)

    //列统计信息
    //    statisticsOperator(sc)

    //相关系数
    //    correlationCoefficient(sc)

    // 卡方检验
    chiSquareCheck(sc);

    sc.stop()
  }

  /**
   * 卡方检验
   *
   * @param sc
   */
  def chiSquareCheck(sc: SparkContext) = {
    val v1 = Vectors.dense(43.0, 9.0)
    val v2 = Vectors.dense(44.0, 4.0)
    val c1 = Statistics.chiSqTest(v1, v2)

    println("卡方检验:" + c1)
  }

  /**
   * 相关系数
   *
   * @param sc
   */
  def correlationCoefficient(sc: SparkContext) = {
    val data_path = "input/sample_stat.txt"
    val data = sc.textFile(data_path).map(_.split("\t")).map(f => f.map(f => f.toDouble))
    val data1 = data.map(f => Vectors.dense(f))
    val corr1 = Statistics.corr(data1, "pearson")
    val corr2 = Statistics.corr(data1, "spearman")
    val x1 = sc.parallelize(Array(1.0, 2.0, 3.0, 4.0))
    val y1 = sc.parallelize(Array(5.0, 6.0, 6.0, 6.0))
    val corr3 = Statistics.corr(x1, y1, "pearson")

    println("corr1:" + corr1)
    println("corr2:" + corr2)
    println("corr3:" + corr3)


  }

  /**
   * 列统计信息
   *
   * @param sc
   */
  def statisticsOperator(sc: SparkContext) = {
    //列统计汇总
    val data_path = "input/sample_stat.txt"
    val data = sc.textFile(data_path).map(_.split("\t")).map(f => f.map(f => f.toDouble))
    val data1 = data.map(f => Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
    println("最大值为：" + stat1.max)
    println("最小值为：" + stat1.min)
    println("平均值为：" + stat1.mean)
    println("方差值为：" + stat1.variance)
    println("L1范数为：" + stat1.normL1)
    println("L2范数为：" + stat1.normL2)
  }

}