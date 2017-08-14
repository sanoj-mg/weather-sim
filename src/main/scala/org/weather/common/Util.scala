package org.weather.common

import java.text.SimpleDateFormat
import java.util.Properties
import java.util.TimeZone

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.io.Source
import scala.tools.nsc.interpreter.InputStream
import scala.util.Random

/**
  * Created by sanoj on 11/8/17.
  */
object Util {
  // iso8601 dateformat used while printing weather data
  lazy val iso8601dateFormat = {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
    df.setTimeZone(TimeZone.getTimeZone("UTC"))
    df
  }

  // Run spark driver, in standalone mode
  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local[2]")
    .setAppName("Weather Data Generator")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("WARN")
  @transient lazy val sqlContext: SQLContext = new SQLContext(sc)

  // case class to store predefined weather locations
  case class Location(id: Int, fileCode: String, name: String, latitude: Double, longitude: Double, altitude: Int)
  lazy val random: Random = new Random(System.currentTimeMillis)

  // Load the predefined weather locations
  lazy val location: Map[Int, Location] = {
    readLocatons("/location.txt")
  }

  def readLocatons(fileName: String): Map[Int, Location] = {
    val stream = this.getClass.getResourceAsStream(fileName)
    using(Source.fromInputStream(stream)) { source: Source  => {
      source.getLines().map(line => line.split(" +"))
        .map(w => (w(0).toInt, new Location(w(0).toInt, w(1), w(2), w(3).toDouble, w(4).toDouble, w(5).toInt))).toMap
    }}
  }

  // Configuration parameters
  lazy val config: Properties = {
    val stream = this.getClass.getResourceAsStream("/weather.properties")
    using(stream) { stream: InputStream  => {
      val p = new Properties()
      p.load(stream)
      p
    }}
  }

  // A function to properly close the resource
  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B = {
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }

  // Delete file from Hadoop compatible filesystem
  def deleteFile(filepath: String): Unit = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      fs.delete(new org.apache.hadoop.fs.Path(filepath), true)
    } catch {
      case _ : Throwable => { }
    }
  }
}
