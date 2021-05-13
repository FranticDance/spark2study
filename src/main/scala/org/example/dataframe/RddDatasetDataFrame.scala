package org.example.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * @author lee
 * @date 2021/5/13 上午11:00
 * @version 1.0
 */
object RddDatasetDataFrame {
  val session = SparkSession
    .builder().appName("test").master("local").getOrCreate()

  import session.implicits._

  def main(args: Array[String]): Unit = {
    //rddToDataframe

    //dataframeToDataset

    rddToDataset
    session.close()
  }

  def rddToDataframe(): Unit = {
    val rdd: RDD[(Int, String, String)] = session.sparkContext.makeRDD(List((1, "12", "李四"), (2, "33", "王五"), (3, "54", "赵六")))
    val dataFrame = rdd.toDF("id", "age", "name")
    dataFrame.show()

    val rdd1: RDD[Row] = dataFrame.rdd
  }
  def dataframeToDataset(): Unit = {
    val rdd: RDD[(Int, String, String)] = session.sparkContext.makeRDD(List((1, "12", "李四"), (2, "33", "王五"), (3, "54", "赵六")))
    val dataFrame = rdd.toDF("id", "age", "name")

    val dataset: Dataset[User] = dataFrame.as[User]
    dataset.show()
  }
  def rddToDataset(): Unit ={
    val rdd: RDD[(Int, String, String)] = session.sparkContext.makeRDD(List((1, "12", "李四"), (2, "33", "王五"), (3, "54", "赵六")))
    val dataset = rdd.map({
      case (id, age, name) => User(id, age, name)
    }).toDS()
    dataset.show()
  }

  case class User(id:Int, age:String, name:String)
}
