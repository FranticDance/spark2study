package org.example.dataframe

import org.apache.spark.sql.SparkSession
import org.example.dataframe.TestDSL.getResourcePath
import org.example.dataframe.TestDataSet.session

/**
 * @author lee
 * @date 2021/5/13 上午11:38
 * @version 1.0
 */
object TestUDF {
  val session = SparkSession
    .builder().appName("test").master("local").getOrCreate()

  import session.implicits._
  def main(args: Array[String]): Unit = {
    val str = getResourcePath("people.json")
    val frame = session.read.json(str)
    frame.createOrReplaceTempView("user")
    session.sql("select * from user").show()

    println("=========")
    session.udf.register("myfunc", (name:String) => {"name:" + name})
    session.sql("select age, myfunc(name) from user").show()

    session.close()
  }
}
