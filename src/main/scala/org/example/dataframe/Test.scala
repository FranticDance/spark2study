package org.example.dataframe

import org.apache.spark.sql.SparkSession

/**
 * @author lee
 * @date 2020/12/22 17:25
 * @version 1.0
 */
object Test{
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder().appName("test").master("local").getOrCreate()

    val str = getResourcePath("people.json")

    val frame = session.read.json(str)
    val frame1 = session.read.text(str)

    frame.show()
    frame.printSchema()

    frame.select(frame("age") + 1, frame("name")).show()
    //frame1.show()
  }

  def getResourcePath(name: String): String = {
    //val stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("people.json")
    val loader = Thread.currentThread().getContextClassLoader()
    loader.getResource(name).getPath
  }
}
