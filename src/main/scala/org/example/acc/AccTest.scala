package org.example.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.example.partitioner.PartitionerTest.Mypartitioner

/**
 * @author lee
 * @date 2021/4/29 21:49
 * @version 1.0
 */
object AccTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)
    val myacc = context.longAccumulator("myacc")
    val rdd = context.parallelize(List(1,2,3,4))
    rdd.foreach(num => {
      myacc.add(num)
    })
    println(myacc.value)
    context.stop()
  }

}
