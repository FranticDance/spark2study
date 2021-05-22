package org.example.rdd

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lee
 * @date 2021/4/29 上午11:35
 * @version 1.0
 */
object RddTest5 {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File("output1"))
    FileUtils.deleteQuietly(new File("output2"))
    FileUtils.deleteQuietly(new File("output3"))
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(conf)
    val rdd = context.textFile(getResourcePath("helloword.txt"), 3)
    //println(rdd.dependencies)
    //val rddr = rdd.repartition(5)
    //rdd.saveAsTextFile("output3")
    val value = rdd.repartition(5)
    println(value.toDebugString)
    println(value.dependencies)
    println("=======22222========")
    println(rdd.dependencies)
    println("===============")
    val rdd1 = rdd.flatMap(_.split(","))
    println(rdd1.dependencies)
    println("===============")
    //rdd1.repartition(5)
    rdd1.saveAsTextFile("output1")
    val rdd2 = rdd1.map((_, 1))
    println(rdd2.dependencies)
    println("===============")
    val resultRdd = rdd2.reduceByKey(_ + _, 5)
    resultRdd.saveAsTextFile("output2")
    println(resultRdd.dependencies)
    println("===============")
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
