package ssa

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import java.lang.Double.isNaN
import main.NAStatCounter

@main def run() = Main.run()

case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)
case class Scored(md: MatchData, score: Double)

// レコードのリンク問題を解決
object Main:
  def isHeader(line: String): Boolean = line.contains("id_1")
  def toDouble(s: String) = if ("?".equals(s)) Double.NaN else s.toDouble
  // 読み込んだ　csv の１行を split して、数値型を変換.
  // '?' は raw データで不明なフィールドに入っているため、NaN に変換
  // id1 患者1 の id, id2 患者2 の id
  // scores: それぞれの患者のスコアがどれくらいマッチしているか
  // matched: それぞれが同じ患者かどうか
  def parse(line: String): MatchData = {
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }

  def statWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] =
    val nasRDD = rdd.map(_.map(NAStatCounter(_)))
    nasRDD.reduce(_.zip(_).map { case (a, b) => a.merge(b) })

  def statWithMissingOpt(rdd: RDD[Array[Double]]): Array[NAStatCounter] =
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) =>
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => nas.zip(arr).foreach { case (nas, d) => nas.add(d) })
      Iterator(nas)
    )
    nastats.reduce((n1, n2) => n1.zip(n2).map { case (a, b) => a.merge(b) })

  def run(): Unit =
    val conf = new SparkConf().setAppName("Rdd Project").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rawBlocks = sc.textFile(getClass.getResource("/linkage").getPath())
    val parsed = rawBlocks.filter(!isHeader(_)).map(parse(_))

    // parsed.map(parse(_).matched).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    // 要約統計のため　NaN を取り除く
    // println(parsed.map(parse(_).scores(0)).filter(!isNaN(_)).stats())

    // 上のように各 field ごとに実行すると非効率なため、NAStatCounter を使う
    // クラスタ上の RDD[Array[Double]] を RDD[Array[NAStatCounter]] へマップ ->
    // 単一の RDD と acc を zip して、各 field を集計.
    // NAStatCounter により、単一のループで NaN を弾いて集計
    // statWithMissing(parsed.map(_.scores)).foreach(println)

    // val statsm = statWithMissingOpt(parsed.filter(_.matched).map(_.scores))
    // val statsn = statWithMissingOpt(parsed.filter(!_.matched).map(_.scores))

    // 欠損値が少なく、平均値の差が大きい field を探す -> 2, 5, 6, 7, 8
    // statsm
    //   .zip(statsn)
    //   .map { case (a, b) =>
    //     (a.missing + b.missing, a.stats.mean - b.stats.mean)
    //   }
    //   .foreach(println)

    expect(parsed)

  def expect(rdd: RDD[MatchData]) =
    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d
    val ct = rdd.map(md =>
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    )
    // Map(true -> 20871, false -> 637)
    println(ct.filter(s => s.score >= 4.0).map(s => s.md.matched).countByValue())
