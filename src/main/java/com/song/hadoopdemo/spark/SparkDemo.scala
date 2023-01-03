package com.song.hadoopdemo.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark测试Demo
 */
object SparkDemo {

  def main(args: Array[String]): Unit = {
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    // 创建 Spark 上下文环境对象（连接对象）
    val sparkContext = new SparkContext(sparkConf)

    //从集合（内存）中创建 RDD
    //    getDataByMemory(sparkContext)


    //从外部存储（文件）创建 RDD
    //getDataByOutFile(sparkContext);

    //RDD 转换算子
    //tranOperator(sparkContext);

    //扁平化映射处理
    //flatMapOperator(sparkContext);


    //分组
    //groupByOperator(sparkContext)

    //过滤
    //filterOperator(sparkContext)

    //根据指定的规则从数据集中抽取数据
    //sampleOperator(sparkContext)

    //去重
    //distinctOperator(sparkContext)

    //排序
    //sortByOperator(sparkContext)

    //交集
    //intersectionOperator(sparkContext)

    //并集
    //unionOperator(sparkContext);

    //差集
    //subtractOperator(sparkContext)

    //将两个 RDD 中的元素，以键值对的形式进行合并
    //zipOperator(sparkContext)

    //将数据按照相同的 Key 对 Value 进行聚合
    //reduceByKeyOperator(sparkContext)

    //返回一个由 RDD 的前 n 个元素组成的数组
    //takeOperator(sparkContext)

    //聚合操作
    aggregateOperator(sparkContext)

    //关闭 Spark 连接
    sparkContext.stop()
  }

  /**
   * 返回一个由 RDD 的前 n 个元素组成的数组
   * 10
   *
   * @param sparkContext
   */
  def aggregateOperator(sparkContext: SparkContext) = {
    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4), 1)
    // 将该 RDD 所有元素相加得到结果
    val result: Int = rdd.aggregate(0)(_ + _, _ + _)

    println("result:" + result)
  }

  /**
   * 返回一个由 RDD 的前 n 个元素组成的数组
   * 1,2
   *
   * @param sparkContext
   */
  def takeOperator(sparkContext: SparkContext) = {
    val rdd: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))
    // 返回 RDD 中元素的个数
    val takeResult: Array[Int] = rdd.take(2)
    println(takeResult.mkString(","))

  }

  /**
   * 将数据按照相同的 Key 对 Value 进行聚合
   * (a,6)
   * (b,4)
   *
   * @param sparkContext
   */
  def reduceByKeyOperator(sparkContext: SparkContext) = {
    val rdd = sparkContext.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4)
    ))

    // reduceByKey : 相同的key的数据进行value数据的聚合操作
    // scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
    // 【1，2，3】
    // 【3，3】
    // 【6】
    // reduceByKey中如果key的数据只有一个，是不会参与运算的。
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = ${x}, y = ${y}")
      x + y
    })
    reduceRDD.collect().foreach(println)
  }

  /**
   * 将两个 RDD 中的元素，以键值对的形式进行合并
   * (1,3)
   * (2,4)
   * (3,5)
   * (4,6)
   */
  def zipOperator(sparkContext: SparkContext) = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sparkContext.makeRDD(List(3, 4, 5, 6))
    val dataRDD = dataRDD1.zip(dataRDD2)
    dataRDD.collect.foreach(println)
  }


  /**
   * 差集
   * 1,2
   *
   * @param sparkContext
   */
  def subtractOperator(sparkContext: SparkContext) = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sparkContext.makeRDD(List(3, 4, 5, 6))
    val dataRDD = dataRDD1.subtract(dataRDD2)
    dataRDD.collect.foreach(println)
  }

  /**
   * 并集
   * 1,2,3,4,5,6
   *
   * @param sparkContext
   */
  def unionOperator(sparkContext: SparkContext) = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sparkContext.makeRDD(List(3, 4, 5, 6))
    val dataRDD = dataRDD1.union(dataRDD2)
    dataRDD.collect.foreach(println)
  }

  /**
   * 交集
   * 3,4
   *
   * @param sparkContext
   */
  def intersectionOperator(sparkContext: SparkContext) = {
    val dataRDD1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD2 = sparkContext.makeRDD(List(3, 4, 5, 6))
    val dataRDD = dataRDD1.intersection(dataRDD2)
    dataRDD.collect.foreach(println)
  }

  /**
   * 排序
   * 1,1,2,2,3,4
   *
   * @param sparkContext
   */
  def sortByOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4, 1, 2
    ), 2)
    val dataRDD1 = dataRDD.sortBy(num => num, true, 4)
    dataRDD1.collect.foreach(println)
  }


  /**
   * 将数据集中重复的数据去重
   * 1,  3, 4,  2
   *
   * @param sparkContext
   */
  def distinctOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4, 1, 2
    ), 1)
    //    val dataRDD1 = dataRDD.distinct()
    val dataRDD2 = dataRDD.distinct(2)

    //    dataRDD1.collect().foreach(println)
    dataRDD2.collect.foreach(println)
  }

  /**
   * 样本抽取
   *
   * @param sparkContext
   */
  def sampleOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4
    ), 1)

    // 抽取数据不放回（伯努利算法）
    // 伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子
    //    val dataRDD1 = dataRDD.sample(false, 0.5)
    // 抽取数据放回（泊松算法）
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    val dataRDD2 = dataRDD.sample(true, 1)

    //    dataRDD1.collect().foreach(println)
    println("****************************")
    dataRDD2.collect().foreach(println)
  }

  /**
   * 过滤
   *
   * @param sparkContext
   */
  def filterOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(
      1, 2, 3, 4
    ), 1)
    val dataRDD1 = dataRDD.filter(_ % 2 == 0)
    //输出
    dataRDD1.collect().foreach(println);
  }

  /**
   * 分组
   *
   * @param sparkContext
   */
  def groupByOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(1, 2, 3, 4), 1)
    val dataRDD1 = dataRDD.groupBy(
      _ % 2
    )
    //输出
    dataRDD1.collect().foreach(println);
  }


  /**
   * 扁平化映射处理
   * 1,2,3,4
   *
   * @param sparkContext
   */
  def flatMapOperator(sparkContext: SparkContext) = {
    val dataRDD = sparkContext.makeRDD(List(
      List(1, 2), List(3, 4)
    ), 1)
    val dataRDD1 = dataRDD.flatMap(
      list => list
    )

    //输出
    dataRDD1.collect().foreach(println);
  }


  /**
   * RDD 转换算子
   * 2,4,6,8
   */
  def tranOperator(sparkContext: SparkContext) = {
    val dataRDD: RDD[Int] = sparkContext.makeRDD(List(1, 2, 3, 4))
    val dataRDD1: RDD[Int] = dataRDD.map(
      num => {
        num * 2
      }
    )
    val dataRDD2: RDD[String] = dataRDD1.map(
      num => {
        "" + num
      }
    )

    //输出
    dataRDD2.collect().foreach(println);
  }

  /**
   * 从外部存储（文件）创建 RDD
   *
   * @param sparkContext
   */
  def getDataByOutFile(sparkContext: SparkContext) = {
    val fileRDD: RDD[String] = sparkContext.textFile("input/wordCount.txt")
    fileRDD.collect().foreach(println)
  }

  /**
   * 从集合（内存）中创建 RDD
   *
   * @param sparkContext
   */
  def getDataByMemory(sparkContext: SparkContext) = {
    //从集合（内存）中创建 RDD
    val rdd1 = sparkContext.parallelize(
      List(1, 2, 3, 4)
    )
    val rdd2 = sparkContext.makeRDD(
      List(1, 2, 3, 4)
    )
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
  }

}
