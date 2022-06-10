package ssa

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

@main def run() = Main.run()

object Main {
  def isHeader(line: String): Boolean = line.contains("id_1")

  def run(): Unit =
    val conf = new SparkConf().setAppName("Rdd Project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rawBlocks = sc.textFile(getClass.getResource("/linkage").getPath())

    rawBlocks.filter(!isHeader(_)).take(10).foreach(println)
}
