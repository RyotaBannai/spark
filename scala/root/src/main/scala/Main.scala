import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

// Spark Intro
// https://dev.classmethod.jp/articles/apache-spark_rdd_investigation/

@main def run() = Main.run()

object Main {
  def printRDD(filterName: String, rdd: org.apache.spark.rdd.RDD[_]) = {
    println(filterName)
    rdd.foreach { r => { println(r) } }
  }

  def run(): Unit = {
    val conf = new SparkConf().setAppName("Rdd Project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(getClass.getResource("/ken_all_rome.csv").getPath())

    val addresses = inputRDD.map { line =>
      val splited = line.replace("\"", "").split(",")
      var result: Array[String] = null
      if (splited(6) == "IKANIKEISAIGANAIBAAI")
        result = Array(splited(0), splited(4), splited(5))
      else result = Array(splited(0), splited(4), splited(5), splited(6))

      result.mkString(" ")
    }

    printRDD("mapped RDD", addresses)
  }
}
