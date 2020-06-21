package com.machinedoll.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.types.Row


object StreamExample {
  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings)

    val customStream = fsEnv.fromCollection(Seq(1, 2, 3, 1, 2, 3, 4, 5))

    val simpleTable: Table = fsTableEnv.fromDataStream(customStream).select("*")

    val exampleStream = fsTableEnv.toRetractStream[Row](simpleTable)

    exampleStream.print()

    fsEnv.execute("Stream Example")
  }
}
