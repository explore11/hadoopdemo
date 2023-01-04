package com.song.hadoopdemo.sparkmllib

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.{KMeansDataGenerator, LinearDataGenerator, LogisticRegressionDataGenerator, MLUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark MLlib Statistics 统计操作
 */
object rdd_test {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rdd_test_2.2.1")
    val sc = new SparkContext(conf)

    //列统计信息
    //statisticsOperator(sc)

    //相关系数
    //correlationCoefficient(sc)

    //卡方检验
    //chiSquareCheck(sc);

    //加载LIBSVM格式的文件
    //loadLibSvmFile(sc)

    //保存LIBSVM格式的文件
    //saveLibSvmFile(sc)

    //生成样本
    //getSample(sc)
    sc.stop()
  }

  /**
   * 生成样本
   *
   * @param sc
   */
  def getSample(sc: SparkContext) = {
    println("================KMeans===================")
    /**
     * 用于生成KMeans的训练样本数据  随机40个样本 聚类中心是5 数据维度为3 初始中心分布的比例因子为1.0 数据分区为2
     */
    val KMeansRDD = KMeansDataGenerator.generateKMeansRDD(sc, 40, 5, 3, 1.0, 2)
    println("KMeansRDD总数：" + KMeansRDD.count())
    for (elem <- KMeansRDD.take(5)) {
      elem.foreach(println)
    }


    println("================Linear===================")
    /**
     * 用于生成线性回归的训练样本数据  随机40个样本 样本的特征数为3 缩放实例的Epsilon的因子为1.0 数据分区为2
     */
    val LinearRDD = LinearDataGenerator.generateLinearRDD(sc, 40, 3, 1.0, 2, 0.0)
    println("Linear总数：" + LinearRDD.count())
    LinearRDD.take(5).foreach(println)


    println("================Logistic===================")
    /**
     * 用于逻辑回归的训练样本数据  随机40个样本 样本的特征数为3 缩放实例的Epsilon的因子为1.0 数据分区为2 标签1的概率为0.5
     */
    val LogisticRDD = LogisticRegressionDataGenerator.generateLogisticRDD(sc, 40, 3, 1.0, 2, 0.5)
    println("Logistic总数" + LogisticRDD.count())
    LogisticRDD.take(5).foreach(println)

  }

  /**
   * 保存LIBSVM格式的文件
   *
   * @param sc
   */
  def saveLibSvmFile(sc: SparkContext): Unit = {
    val data_path = "input/sample_libsvm_data.txt"
    //读取
    val rdd: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, data_path)
    //保存
    MLUtils.saveAsLibSVMFile(rdd, "output/sample_libsvm_data_out.txt");

  }

  /**
   * 加载LIBSVM格式的文件
   *
   * @param sc
   */
  def loadLibSvmFile(sc: SparkContext): Unit = {
    val data_path = "input/sample_libsvm_data.txt"
    val rdd: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, data_path)
    rdd.collect().foreach(println)
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