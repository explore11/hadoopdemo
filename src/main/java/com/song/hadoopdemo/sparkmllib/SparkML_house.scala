package com.song.hadoopdemo.sparkmllib

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 线性回归: 带有文件头的方式导入文件数据
 */

object SparkML_house {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("linear").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 如果是已经处理好的结构化数据, 则可以直接使用这种方式读入数据, 但仍需要一些处理
    // 文件读取出来就是 DataFrame 格式, 而不是 RDD 格式
    val file: DataFrame = spark.read.format("csv").option("sep", ";").option("header", "true").load("data/house.csv")
    file.show()

    import spark.implicits._
    // 对 DtaFrame 中的数据进行筛选与处理, 并最后转化为一个新的 DataFrame
    val dataPre: Dataset[(Int, String, Double, Double)] = file.select("position", "type", "square", "price")
      .map(row => (row.getAs[String](0).toInt,row.getAs[String](1),row.getAs[String](2).toDouble, row.getString(3).toDouble))
    val data: DataFrame = dataPre.toDF("position", "type", "square", "price")
    data.show()

    // VectorAssembler 是一个转换器
    val assembler = new VectorAssembler().setInputCols(Array("position","square")).setOutputCol("features")
    val dataset = assembler.transform(data)

    dataset.show()

    //拆分成训练集和测试集
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2), 1)

    // 设置线性回归参数
    val lr = new LinearRegression()
      .setLabelCol("price")
      .setFeaturesCol("features")
      .setFitIntercept(true) // 是否有截距
      .setMaxIter(1000) // 最大迭代次数
      .setRegParam(0.3) // 设置正则化参数
      .setElasticNetParam(0.8)

    //训练模型
    val model = lr.fit(train)
    //预测
    val result = model.transform(test)

    //展示
    result.show()

    /*
    fit 做训练
    transform 做预测
     */
  }
}
