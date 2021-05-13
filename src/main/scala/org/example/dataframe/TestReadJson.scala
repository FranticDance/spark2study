package org.example.dataframe

import org.apache.spark.sql.SparkSession

/**
 * @author lee
 * @date 2020/12/22 17:25
 * @version 1.0
 */
object TestReadJson{
  def main(args: Array[String]): Unit = {

    val session = SparkSession
      .builder().appName("test").master("local").getOrCreate()
    val str = getResourcePath("people.json")

    //这样读进来是一个二维表的形式
    val frame = session.read.json(str)
    //这样读进来是一个字符串的形式
    val frame1 = session.read.text(str)

    println("frame1===========")
    frame1.show()
    println("frame===========")
    frame.show()
    frame.printSchema()


    //frame1.show()

    session.close()
  }

  def getResourcePath(name: String): String = {
    //val stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("people.json")
    val loader = Thread.currentThread().getContextClassLoader()
    loader.getResource(name).getPath
  }
}
