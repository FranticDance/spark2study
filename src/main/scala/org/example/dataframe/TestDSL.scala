package org.example.dataframe

import org.apache.spark.sql.SparkSession

/**
 * @author lee
 * @date 2021/5/13 上午10:28
 * @version 1.0
 */
object TestDSL {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder().appName("test").master("local").getOrCreate()

    val str = getResourcePath("people.json")

    val frame = session.read.json(str)
    //除了使用SQL语法来查询，还可以使用下面这种DSL语法
    frame.select("age", "name").show()

    //如果需要对列进行转换，使用DSL语法，类似下面那样用$加列名来获取列的话，需要引入隐式转换.建议在创建sparksession后马上
    //导入隐式转换，免得遗忘
    import session.implicits._
    frame.select($"age" + 2 as("newage"), $"name").show()
    //也可以用单引号来代替$
    frame.select('age * 2 as "newage2", 'name).show()


    println("================")
    //如果不引入隐式转换，需要这样来使用
    frame.select(frame("age") + 1, frame("name")).show()

    session.close()
  }

  def getResourcePath(name: String): String = {
    //val stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("people.json")
    val loader = Thread.currentThread().getContextClassLoader()
    loader.getResource(name).getPath
  }
}
