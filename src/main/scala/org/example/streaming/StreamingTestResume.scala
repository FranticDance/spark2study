package org.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author lee
 * @date 2021/5/17 上午11:08
 * @version 1.0
 */
object StreamingTestResume {
  def main(args: Array[String]): Unit = {

    //优雅关闭后恢复数据
    val streamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")

      val streamingContext = new StreamingContext(sparkConf, Seconds(2))
      val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
      val wordMap = lines.map((_, 1))
      val value: DStream[(String, Int)] = wordMap.reduceByKeyAndWindow((x, y) => {
        x + y
      }, (x, y) => {
        //模拟处理时延情况
        //Thread.sleep(4000)
        x - y
      }, Seconds(8), Seconds(2))
      value.print()
      streamingContext
    })

    streamingContext.checkpoint("cp")


   /* lines.foreachRDD(rdd => {
      println("即使rdd为空也是输出")
      Thread.sleep(4  *  1000 )
    })*/

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(1000 * 10)
        streamingContext.stop(true, true)
      }
    }).start()



    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
