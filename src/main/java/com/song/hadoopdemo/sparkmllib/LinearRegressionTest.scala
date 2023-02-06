//package com.song.hadoopdemo.sparkmllib
//
//import org.apache.spark.ml.regression.LinearRegression
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
//import org.apache.spark.sql.SparkSession
//
//class LinearRegressionTest {
//  def main(args: Array[String]): Unit = {
//    //创建 SparkSession 对象
//    val spark = SparkSession.builder.master("local[*]").appName("LinearRegressionWithSGD").getOrCreate()
//
//    val data_path1 = "data/ridge-data/lpsa.data"
//    //读取数据
//    val data = spark.read.textFile(data_path1)
//    val labelePointData = data.map { line =>
//      val parts = line.split(',')
//      val y = parts(0)
//      val x = parts(1)
//      LabeledPoint(y.toDouble, Vectors.dense(x.split(' ').map(_.toDouble)))
//    }.cache()
//    //缓存
//    //准备训练集和测试集
//    val trainTestData = labelePointData.randomSplit(Array(0.8, 0.2), 1)
//    /*
//    * 迭代次数
//    * 训练一个多元线性回归模型收敛（停止迭代）条件：
//    * 1、error值小于用户指定的error值
//    * 2、达到一定的迭代次数
//    */
//    //迭代次数阈值
//    val tterationNums = 1000
//    // 梯度下降算法的下降步长大小 0.001 0.0001
//    val step = 0.001
//    //每次theta更新后计算误差选择的样本数量
//    val batchFraction = 1
//    //构建线性回归模型
//    val lr = new LinearRegression()
//    //截距
//    lr.setFitIntercept(true)
//    //迭代次数
//    lr.setMaxIter(tterationNums)
//    lr.setEpsilon()
//    //步长
//
//    //1代表所有样本,默认就是1.0
//    lr.optimizer.setMiniBatchFraction(batchFraction)
//    //训练模型
//    val model = lr.fit(trainTestData(0))
//    //weights:权重
//    println(model)
//    println(model.intercept)
//    // 对样本进行测试
//    val prediction = model.predict(trainTestData(1).map(_.features))
//    val predictionAndLabel = prediction.zip(trainTestData(1).map(_.label))
//    val print_predict = predictionAndLabel.take(10)
//    for (i <- 0 to print_predict.length - 1) {
//      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
//    }
//    // 计算测试集平均误差
//    val loss = predictionAndLabel.map {
//      case (p, v) =>
//        val err = p - v
//        Math.abs(err)
//    }.reduce(_ + _)
//    val error = loss / trainTestData(1).count
//    println(s"Test data RMSE = " + error)
//    spark.stop()
//  }
//}
//
