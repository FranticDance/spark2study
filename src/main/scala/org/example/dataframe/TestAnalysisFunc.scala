package org.example.dataframe

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 测试分析函数
 * @author lee
 * @date 2021/5/17 下午6:03
 * @version 1.0
 */
object TestAnalysisFunc {
  val session = SparkSession
    .builder().appName("test").master("local").getOrCreate()

  import session.implicits._

  case class Emp(empId: Int, deptId: Int, deptName: String, salary: Double)

  def main(args: Array[String]): Unit = {
    val rdd: RDD[(Int, Int, String, Double)] = session.sparkContext.makeRDD(List(
      (1, 10, "市场部", 5000.00),
      (2, 10, "市场部", 4500.00),
      (3, 20, "科技部", 5000.00),
      (4, 20, "科技部", 6000.00),
      (5, 30, "财务部", 4000.00),
      (6, 30, "财务部", 3000.00),
      (7, 30, "财务部", 4500.00)
    ))
    val dataFrame = rdd.toDF("empid", "deptid", "deptname", "salary")

    val ds: Dataset[Emp] = dataFrame.as[Emp]
    ds.createOrReplaceTempView("emp")
    //ds.show()

    val frame = session.sql("select  empid,deptid,deptname,salary, ROW_NUMBER() OVER(partition by deptid order by salary desc) as ParNumber," +
      " ROW_NUMBER() OVER( order by deptid,salary desc) as RowNumber from emp")

    frame.show()

    session.close()
  }
}
