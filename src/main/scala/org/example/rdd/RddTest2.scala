package org.example.rdd

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}



/**
 * @author lee
 * @date 2021/4/28 上午10:04
 * @version 1.0
 */
object RddTest2 {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File("output"));
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)
    val value = context.textFile("datas/file1.txt")
    value.saveAsTextFile("output")
    context.stop()
  }
}
