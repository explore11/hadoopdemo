package com.song.hadoopdemo.sparkmllib

import breeze.linalg._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * ScalaNLP 是一套机器学习核数值计算的库 主要是科学计算（SC）、机器学习（ML）、自然语言处理（NLP）
 * 对应了三个库 Breeze,Epic,Puck
 *
 */
object breeze_test {

  def main(args: Array[String]): Unit = {
    //环境配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("breeze_test01_3.1.1")
    val sc = new SparkContext(conf)

    //Breeze 创建函数
    //createFunction()

    //计算函数
    //calculationFunction()

    //求和函数
    //calSumFunction()


    //布尔函数
    //boolFunction()

    //线性代数函数
    //LinearAlgebraFunction()

    sc.stop()
  }

  /**
   * 线性代数函数
   */
  def LinearAlgebraFunction() = {
    // 线性代数函数
    val dataOne = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
    val dataTwo = DenseMatrix((1.0, 1.0, 1.0), (1.0, 1.0, 1.0), (1.0, 1.0, 1.0))
    //线性求解
    println(dataOne \ dataTwo)
    //转置
    println(dataOne.t)
    //求特征值
    println(det(dataOne))
    //求逆
    println(inv(dataOne))
    //奇异值分解
    val svd.SVD(u, s, v) = svd(dataOne)
    println(svd.SVD(u, s, v))
    //矩阵行数
    println(dataOne.rows)
    //矩阵列数
    println(dataOne.cols)
  }

  /**
   * 布尔函数
   */
  def boolFunction() = {
    val boolData = DenseVector(true, false, true)
    //元素非操作
    println(!boolData)

    val data = DenseVector(1.0, 0.0, -2.0)
    //任意元素非零
    println(any(data))
    //所有元素非零
    println(all(data))
  }

  /**
   * 求和函数
   */
  def calSumFunction() = {
    val data = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0), (7.0, 8.0, 9.0))
    //元素求和
    println(sum(data))
    //元素每一列求和
    println(sum(data, Axis._0))
    //元素每一行求和
    println(sum(data, Axis._1))
    //对角线元素和
    println(trace(data))
    //累计元素和
    println(accumulate(DenseVector(1, 2, 3, 4)))
  }


  /**
   * 计算函数
   */
  def calculationFunction() = {
    val data1 = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    val data2 = DenseMatrix((1.0, 1.0, 1.0), (2.0, 2.0, 2.0))
    //加
    println(data1 + data2)
    //减
    println(data1 - data2)

    //元素比较相等
    println(data1 :== data2)
    //元素追加
    println(data1 :+= 1.0)
    //元素追乘
    println(data1 :*= 2.0)

    //最大值
    println(max(data1))
    //最大值以及位置
    println(argmax(data1))
    //点积
    println(DenseVector(1, 2, 3, 4) dot DenseVector(1, 1, 1, 1))
  }

  /**
   * 创建函数
   */
  def createFunction() = {
    // 全0矩阵
    val allZerosMatrix = DenseMatrix.zeros[Double](2, 3)
    println("==================== 全0矩阵 ====================")
    println(allZerosMatrix)


    // 全0向量
    val allZeroVector = DenseVector.zeros[Double](3)
    println("==================== 全0向量 ====================")
    println(allZeroVector)

    // 全1向量
    val allOneVector = DenseVector.ones[Double](3)
    println("==================== 全1向量 ====================")
    println(allOneVector)


    //单位矩阵
    val unitMatrix = DenseMatrix.eye[Double](3)
    println("==================== 单位矩阵 ====================")
    println(unitMatrix)


    /**
     * 对角矩阵
     */
    val diagMatrix = diag(DenseVector(1.0, 2.0, 3.0))
    println("==================== 对角矩阵 ====================")
    println(diagMatrix)

    /**
     * 按照行创建矩阵
     */
    val lineCreateMatrix = DenseMatrix((1.0, 2.0), (3.0, 4.0))
    println("==================== 按照行创建矩阵 ====================")
    println(lineCreateMatrix)

    /**
     * 按照行创建向量
     */
    val lineCreateVector = DenseVector(1, 2, 3, 4)
    println("==================== 按照行创建向量 ====================")
    println(lineCreateVector)
  }
}

