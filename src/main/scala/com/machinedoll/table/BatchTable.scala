package com.machinedoll.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

import org.apache.flink.api.scala._

object BatchTable {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val path = this.getClass.getClassLoader.getResource("words.txt").getPath

    val schema = new Schema()
      .field("a", DataTypes.INT())
      .field("b", DataTypes.STRING())

    tEnv.connect(new FileSystem().path(path))
      .withFormat(new Csv().fieldDelimiter(','))
      .withSchema(schema)
      .createTemporaryTable("table1")

    val table = tEnv.from("table1").groupBy("b").select("b, b.count as count")

    val tableDataset = tEnv.toDataSet[(String, Long)](table)

    tableDataset.print()
  }
}
