package org.example.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.example.acc.AccErrorTest.error2

import scala.collection.mutable

/**
 * @author lee
 * @date 2021/4/29 22:32
 * @version 1.0
 */
object CustomAcc {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)

    val myCustomAcc = new MyCustomAcc
    context.register(myCustomAcc, "myacc")

    val rdd = context.parallelize(List("aa", "bb", "cc", "bb"))

    rdd.foreach(myCustomAcc.add(_))

    println(myCustomAcc.value)
    context.stop()
  }

  class MyCustomAcc extends AccumulatorV2[String, scala.collection.mutable.Map[String, Long]]{
    private var wcMap = mutable.Map[String, Long]()

    /**
     * 判断此累加器是否为初始状态
     * 当wcMap为空的时候，就是初始状态了
     * @return
     */
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    /**
     * 复制一个新的累加器
     * @return
     */
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyCustomAcc
    }

    /**
     * 重置累加器
     */
    override def reset(): Unit = {
      wcMap.clear()
    }

    /**
     * 定义基本的累加逻辑，这个就是每个executor在执行的时候，会使用的累加逻辑
     * @param word
     */
    override def add(word: String): Unit = {
      val newCount = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCount)
    }

    /**
     * driver端创建的累加器会发送到Executor，每个Executor的累加结果会发回到Driver，来由Driver进行Merge合并
     * 这个方法就是定义合并逻辑
     * @param other
     */
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val otherMap: mutable.Map[String, Long] = other.value
      otherMap.foreach{
        case (word,count) => {
          val newCount = this.wcMap.getOrElse(word, 0L) + count
          wcMap.update(word, newCount)
        }
      }
    }

    /**
     * 累加器的结果
     * @return
     */
    override def value: mutable.Map[String, Long] = wcMap
  }

}
