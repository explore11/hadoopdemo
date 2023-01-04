package com.song.hadoopdemo.sparkmllib

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * 逻辑回归和多项式逻辑回归 scala版本
 *
 * 逻辑回归是一种流行的预测分类响应的方法。它是预测结果概率的广义线性模型的特例。
 * 在spark.ml逻辑回归中，可以通过使用二项式逻辑回归来预测二元结果，也可以通过使用多项式逻辑回归来预测多类结果。
 * 使用该family 参数在这两种算法之间进行选择，或者不设置它，Spark 将推断出正确的变体。
 *
 * ------family通过将参数设置为“multinomial”，可以将多项逻辑回归用于二元分类。它将产生两组系数和两个截距。
 *
 * 当在具有常量非零列的数据集上无截距拟合 LogisticRegressionModel时，Spark MLlib 为常量非零列输出零系数。
 *
 *
 * 二元分类训练二项式和多项式逻辑回归模型
 */
object LogisticRegressionByScala {
  def main(args: Array[String]): Unit = {
    // 构建Spark对象
    val conf = new SparkConf().setMaster("local[*]").setAppName("LinearRegressionWithSGD")
    val sc = new SparkContext(conf)
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 加载训练数据
    val training = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 拟合模型
    val lrModel = lr.fit(training)

    // 打印线性回归的系数和截距
    println(s"逻辑回归回归的系数: ${lrModel.coefficients} 逻辑回归的截距: ${lrModel.intercept}")

    println("==============================分割线=============================")

    // 我们还可以使用多项式族进行二分类 多项式逻辑回归
    val mlr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setFamily("multinomial")

    //  拟合模型
    val mlrModel = mlr.fit(training)

    // 打印多项式逻辑回归的系数和截距
    println(s"多项式逻辑回归的系数: ${mlrModel.coefficientMatrix}")
    println(s"多项式逻辑回归的截距: ${mlrModel.interceptVector}")

    //关闭
    sc.stop()
  }
}
