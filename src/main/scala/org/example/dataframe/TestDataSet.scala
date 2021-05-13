package org.example.dataframe

import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.dataframe.TestDSL.getResourcePath

/**
 * @author lee
 * @date 2021/5/13 上午10:41
 * @version 1.0
 */
object TestDataSet {
  val session = SparkSession
    .builder().appName("test").master("local").getOrCreate()
  import session.implicits._

  def main(args: Array[String]): Unit = {
    val str = getResourcePath("people.json")
    val frame = session.read.json(str)

    seqToDataset()

    session.close()
  }

  def seqToDataset() : Unit ={
    val seq = Seq(1, 2, 3, 4)
    val value: Dataset[Int] = seq.toDS()
    value.show()
  }

}
