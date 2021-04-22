package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author lee
 * @date 2020/12/23 14:31
 * @version 1.0
 */
object PipelineTEst {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("pipelineTest")
    //val conf = new SparkConf().setMaster("local-cluster[2,2,200]").setAppName("pipelineTest")
      //.set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val rdd1 = sc.parallelize(Array(1,2,3,4))
    val rdd2: RDD[Int] = rdd1.map { x => { println("map---------" + x)
      x } }
    val rdd3: RDD[Int] = rdd2.filter(x => { println("filter***********" + x)
      true })
    rdd3.collect()
    sc.stop()
  }
}
