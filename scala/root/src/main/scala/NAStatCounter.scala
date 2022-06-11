package main

import org.apache.spark.util.StatCounter
import java.lang.Double.isNaN
import javax.print.attribute.standard.MediaSize.NA
import com.fasterxml.jackson.module.scala.deser.overrides

class NAStatCounter extends Serializable:
  val stats: StatCounter = StatCounter()
  var missing: Long = 0

  def add(x: Double): NAStatCounter =
    if (isNaN(x)) missing += 1 else stats.merge(x)
    this

  def merge(other: NAStatCounter): NAStatCounter =
    stats.merge(other.stats)
    missing += other.missing
    this

  override def toString(): String =
    "stats: " + stats.toString + " NaN:" + missing

object NAStatCounter extends Serializable:
  def apply(x: Double): NAStatCounter =
    new NAStatCounter().add(x)
