package org.example.rdd

import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author lee
 * @date 2021/4/28 上午10:04
 * @version 1.0
 */
object RddTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)
    val list = List(1, 2, 3, 4,5)
    val rdd = context.makeRDD(list, 3)
    val value = rdd.repartition(5)
    value.saveAsTextFile("output4")
    Thread.sleep(5000000)
    context.stop()
  }


}
