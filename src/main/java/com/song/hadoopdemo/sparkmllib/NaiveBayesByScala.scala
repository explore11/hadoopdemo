package com.song.hadoopdemo.sparkmllib

import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

/**
 * 朴素贝叶斯 scala版本
 *
 */
object NaiveBayesByScala {
  def main(args: Array[String]): Unit = {
    //创建 SparkSession 对象
    val spark = SparkSession.builder.master("local[*]").appName("NaiveBayes").getOrCreate

    // 将以 LIBSVM 格式存储的数据加载为 DataFrame
    val data = spark.read.format("libsvm").load("data/sample_libsvm_data.txt")

    // 将数据分成训练集和测试集（保留 30% 用于测试）
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 1234L)

    // 训练 NaiveBayes 模型
    val model = new NaiveBayes().fit(trainingData)

    // 选择要显示的示例行
    val predictions = model.transform(testData)
    predictions.show()

    // 选择（预测，真实标签）并计算测试误差
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println(s"测试准确性: $accuracy")

    spark.stop()
  }
}
