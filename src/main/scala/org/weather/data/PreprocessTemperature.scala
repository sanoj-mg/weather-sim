package org.weather.data

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.weather.common.Util
import org.weather.common.Util._
import org.weather.common.Util.sqlContext.implicits._

/**
  * Created by sanoj on 12/8/17.
  *
  * Preprocess downloaded whether data and extract required fields
  * Prepare and save separate observation data to train models for temperature
  */
object PreprocessTemperature {
  // Preprocess downloaded data files
  def preprocess(): Unit = {
    val downloadDir = config.getProperty("weatherdata.save.directory", "weatherData/downloaded")
    location.keys.foreach(prepareTemperatureData(downloadDir, _))
  }
  // case class to hold temperature observation
  case class TemperatureObservation(month: Double, hour: Double, latitude: Double, longitude: Double,
                            altitude: Double, temperature: Double)
  // Prepare training data to predict temperature
  def prepareTemperatureData(dirName: String, locId: Int): Unit = {
    val temprDir = config.getProperty("process.data.temperature.directory", "weatherData/temperature")
    def getTemperatureObservation(row: Array[String]): List[TemperatureObservation] = {
      try {
        val month = row(1).split("-")(1).toDouble
        // (minTemp, 3)  (maxTemp, 12)  (temp9am, 9)  (temp3pm, 15)
        val lat = location.get(locId).get.latitude
        val lon = location.get(locId).get.longitude
        val alt = location.get(locId).get.altitude
        val minTemp = row(2).trim.toDouble
        val maxTemp = row(3).trim.toDouble
        val temp9am = row(10).trim.toDouble
        val temp3pm = row(16).trim.toDouble
        val obs3am = TemperatureObservation(month, 3, lat, lon, alt, minTemp)
        val obs12pm = TemperatureObservation(month, 12, lat, lon, alt, maxTemp)
        val obs9am = TemperatureObservation(month, 9, lat, lon, alt, temp9am)
        val obs3pm = TemperatureObservation(month, 15, lat, lon, alt, temp3pm)
        List(obs3am, obs12pm, obs9am, obs3pm)
      } catch {
        case ex: Exception => List(TemperatureObservation(-1, 0, 0, 0, 0, 0))
      }
    }
    val lines = sc.textFile(s"$dirName/$locId")
    val obsRDD = lines.filter(l => (l.startsWith(",") && !l.startsWith(",\"Date")))
      .map(_.split(",", -1))
      .filter(_.length >= 22)
      .flatMap(row => getTemperatureObservation(row))
      .filter(_.month >= 1)
      .map(obs => List(obs.month, obs.hour, obs.latitude, obs.longitude, obs.altitude,
                       obs.temperature).mkString(","))
    Util.deleteFile(s"$temprDir/$locId")
    obsRDD.saveAsTextFile(s"$temprDir/$locId")
  }
}
