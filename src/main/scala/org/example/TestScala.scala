package org.example

/**
 * @author lee
 * @date 2020/12/24 17:05
 * @version 1.0
 */
object TestScala {
  private var _applicationId: String = _
  private var _applicationAttemptId: Option[String] = None
  private var _executorMemory: Int = _
  def main(args: Array[String]): Unit = {
    println("result:" + _applicationId)
    println("result2:" + _applicationAttemptId)
    println("result3:" + _executorMemory)

    def sayHi(str: String)={
      println(str)
      hello()
    }

    sayHi("aa")

  }
  def hello() = {
    println(11)
  }
}
