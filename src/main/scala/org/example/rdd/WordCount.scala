package org.example.rdd

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lee
 * @date 2021/4/29 上午11:35
 * @version 1.0
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(conf)
    val rdd = context.textFile(getResourcePath("helloword.txt"), 3)
    val value = rdd.repartition(4)
    val rdd1 = value.flatMap(_.split(","))
    val rdd2 = rdd1.map((_, 1))
    val resultRdd = rdd2.reduceByKey(_ + _,4)
    //println("===========")
    //println(resultRdd.toDebugString)
    resultRdd.collect().foreach(println)
    Thread.sleep(5000000)
    context.stop()
  }

  def getResourcePath(name: String): String = {
    //val stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("people.json")
    val loader = Thread.currentThread().getContextClassLoader()
    loader.getResource(name).getPath
  }
}
