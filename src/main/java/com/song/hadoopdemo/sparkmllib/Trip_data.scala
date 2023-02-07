package com.song.hadoopdemo.sparkmllib

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionTrainingSummary}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.jpmml.sparkml.PMMLBuilder

object Trip_data {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("linear").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 如果是已经处理好的结构化数据, 则可以直接使用这种方式读入数据, 但仍需要一些处理
    // 文件读取出来就是 DataFrame 格式, 而不是 RDD 格式
    val file: DataFrame = spark.read.format("csv").option("sep", ",").option("header", "true").load("data/green_tripdata_2020-06.csv")
    //    file.show()

    import spark.implicits._
    // 对 DtaFrame 中的数据进行筛选与处理, 并最后转化为一个新的 DataFrame
    val dataPre: Dataset[(Int, Int, Int, Int, Int, Double, Double, Double, Double, Double, Double, Double, Double)] = file.select("VendorID", "RatecodeID", "PULocationID", "DOLocationID",
      "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount")
      .map(row => (row.getAs[String](0).toInt, row.getAs[String](1).toInt, row.getAs[String](2).toInt, row.getAs[String](3).toInt
        , row.getAs[String](4).toInt, row.getAs[String](5).toDouble, row.getAs[String](6).toDouble, row.getAs[String](7).toDouble, row.getAs[String](8).toDouble, row.getAs[String](9).toDouble
        , row.getAs[String](10).toDouble, row.getAs[String](11).toDouble, row.getAs[String](12).toDouble))

    val data: DataFrame = dataPre.toDF("VendorID", "RatecodeID", "PULocationID", "DOLocationID",
      "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount")

    //    data.show()

    // VectorAssembler 是一个转换器
    val assembler = new VectorAssembler().setInputCols(Array("VendorID", "RatecodeID", "PULocationID", "DOLocationID",
      "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge")).setOutputCol("features")
    val dataset = assembler.transform(data)

    //    dataset.show()

    //拆分成训练集和测试集
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2), 1)

    // 设置线性回归参数
    val lr = new LinearRegression()
      .setLabelCol("total_amount")
      .setFeaturesCol("features")
      .setFitIntercept(true) // 是否有截距
      .setMaxIter(1000) // 最大迭代次数
      .setRegParam(0.1) // 设置正则化参数
      .setElasticNetParam(0.8)

    //训练模型
    val model = lr.fit(train)

    //预测
    val result = model.transform(test)

    model.save("d:\\spark\\model")
    //展示
    result.show(20)


    //    // 在线性回归训练总结模型上打印出一些指标
    //    val trainingSummary = model.summary
    //    System.out.println("迭代次数: " + trainingSummary.totalIterations)
    //    System.out.println("每次迭代的目标函数: " + Vectors.dense(trainingSummary.objectiveHistory))
    //    // 误差（标签 - 预测值）
    //    val residuals = trainingSummary.residuals
    //    residuals.show(20)
    //    System.out.println("均方根误差: " + trainingSummary.rootMeanSquaredError)
    //    System.out.println("决定系数: " + trainingSummary.r2)

    //关闭
    spark.stop()
  }
}
