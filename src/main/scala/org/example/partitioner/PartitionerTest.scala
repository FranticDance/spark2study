package org.example.partitioner

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author lee
 * @date 2021/4/29 21:03
 * @version 1.0
 */
object PartitionerTest {
  def main(args: Array[String]): Unit = {
    FileUtils.deleteQuietly(new File("output"));
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val context = new SparkContext(sparkConf)
    val rdd = context.parallelize(List(("aa", "1111"), ("bb", "22222"), ("cc", "333333"), ("dd", "444444"), ("ee", "5555555"), ("ff", "6666666")))
    val rddPartiton = rdd.partitionBy(new Mypartitioner)
    rddPartiton.saveAsTextFile("output")
    context.stop()
  }

  //自定义一个分区器
  class Mypartitioner extends Partitioner{
    override def numPartitions: Int = 4

    override def getPartition(key: Any): Int = {
      key match {
        case "aa" => 0
        case "bb" => 1
        case "cc" => 2
        case _ => 3
      }
    }
  }
}
