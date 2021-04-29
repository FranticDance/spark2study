package org.example.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lee
 * @date 2021/4/29 上午11:35
 * @version 1.0
 */
object RddTest4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(conf)
    val rdd = context.textFile(getResourcePath("helloword.txt"))
    println(rdd.toDebugString)
    println("===============")
    val rdd1 = rdd.flatMap(_.split(","))
    println(rdd1.toDebugString)
    println("===============")
    val rdd2 = rdd1.map((_, 1))
    println(rdd2.toDebugString)
    println("===============")
    val resultRdd = rdd2.reduceByKey(_ + _)
    println(resultRdd.toDebugString)
    println("===============")
    resultRdd.collect().foreach(println)
    context.stop()
  }

  def getResourcePath(name: String): String = {
    //val stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("people.json")
    val loader = Thread.currentThread().getContextClassLoader()
    loader.getResource(name).getPath
  }
}
