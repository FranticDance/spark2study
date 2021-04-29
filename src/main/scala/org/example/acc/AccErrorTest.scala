package org.example.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author lee
 * @date 2021/4/29 21:49
 * @version 1.0
 */
object AccErrorTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)
    val myacc: LongAccumulator = context.longAccumulator("myacc")
    val rdd = context.parallelize(List(1,2,3,4))

    //error1(rdd, myacc)
    error2(rdd, myacc)

    context.stop()
  }

  /**
   * 没有action算子导致没有累加上
   * @param rdd
   * @param myacc
   */
  def error1(rdd: RDD[Int], myacc:LongAccumulator)= {
    rdd.map(num =>{
      myacc.add(num)
    })
    println(myacc.value)
  }

  /**
   * 多次调用action算子导致累加多了
   * @param rdd
   * @param myacc
   */
  def error2(rdd: RDD[Int], myacc:LongAccumulator)= {
    val value = rdd.map(num => {
      myacc.add(num)
    })
    value.collect()
    value.collect()

    println(myacc.value)
  }

  /**
   * 一般累加器放在action算子中来操作
   * @param rdd
   * @param myacc
   */
  def right(rdd: RDD[Int], myacc:LongAccumulator)= {
    val value = rdd.foreach(num => {
      myacc.add(num)
    })

    println(myacc.value)
  }

}
