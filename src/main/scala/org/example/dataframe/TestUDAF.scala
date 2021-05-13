package org.example.dataframe

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * @author lee
 * @date 2021/5/13 下午2:56
 * @version 1.0
 */
object TestUDAF {
  def main(args: Array[String]): Unit = {

  }

  class MyFuncUDAF extends UserDefinedAggregateFunction{
    override def inputSchema: StructType = ???

    override def bufferSchema: StructType = ???

    override def dataType: DataType = ???

    override def deterministic: Boolean = ???

    override def initialize(buffer: MutableAggregationBuffer): Unit = ???

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def evaluate(buffer: Row): Any = ???
  }

  class MyFuncNewUDAF extends Aggregator{
    override def zero: Nothing = ???

    override def reduce(b: Nothing, a: Any): Nothing = ???

    override def merge(b1: Nothing, b2: Nothing): Nothing = ???

    override def finish(reduction: Nothing): Nothing = ???

    override def bufferEncoder: Encoder[Nothing] = ???

    override def outputEncoder: Encoder[Nothing] = ???
  }

}
