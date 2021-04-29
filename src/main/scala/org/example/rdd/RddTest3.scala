package org.example.rdd

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author lee
 * @date 2021/4/28 上午10:04
 * @version 1.0
 */
object RddTest3 {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File("output"))
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)

    val rdd = context.parallelize(Array(1, 2, 3, 4, 5), 2)
    test3(rdd)

    context.stop()
  }

  /**
   * 求分区内最大值
   * @param rdd
   */
  def test1(rdd: RDD[Int]): Unit = {
    val mapRdd = rdd.mapPartitions(iter => {
      println(">>>")
      List(iter.max).iterator
    })
    mapRdd.collect().foreach(println)
  }

  /**
   * 指定分区操作
   * @param rdd
   */
  def test2(rdd: RDD[Int]): Unit = {
    val mapRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      println(">>>")
      if (index == 1) {
        List(iter.max).iterator
      } else {
        Nil.iterator
      }
    })
    mapRdd.collect().foreach(println)
  }

  /**
   * 打印出数据的分区
   * @param rdd
   */
  def test3(rdd: RDD[Int]): Unit = {
    val mapRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })
    mapRdd.collect().foreach(println)
  }
}
