package org.weather.data

import org.weather.common.Util
import org.weather.common.Util._

/**
  * Created by sanoj on 12/8/17.
  *
  * Preprocess downloaded whether data and extract required fields
  * Prepare and save separate observation data to train models to predict pressure
  *
  */
object PreprocessPressure {
  // Preprocess downloaded data files
  def preprocess(): Unit = {
    val downloadDir = config.getProperty("weatherdata.save.directory", "weatherData/downloaded")
    location.keys.foreach(preparePressureData(downloadDir, _))
  }
  // Case class to hold pressure observation
  case class PressureObservation(month: Double, hour: Double, latitude: Double, longitude: Double,
                                    altitude: Double, pressure: Double)
  // Prepare training data to predict pressure
  def preparePressureData(dirName: String, locId: Int): Unit = {
    val dataDir = config.getProperty("process.data.pressure.directory", "weatherData/pressure")
    def getPressureObservation(row: Array[String]): List[PressureObservation] = {
      try {
        val month = row(1).split("-")(1).toDouble
        val lat = location.get(locId).get.latitude
        val lon = location.get(locId).get.longitude
        val alt = location.get(locId).get.altitude
        val pres9am = row(15).trim.toDouble
        val pres3pm = row(21).trim.toDouble
        val obs9am = PressureObservation(month, 9, lat, lon, alt, pres9am)
        val obs3pm = PressureObservation(month, 15, lat, lon, alt, pres3pm)
        List(obs9am, obs3pm)
      } catch {
        case ex: Exception => List(PressureObservation(-1, 0, 0, 0, 0, 0))
      }
    }
    val lines = sc.textFile(s"$dirName/$locId")
    val obsRDD = lines
      .filter(l => (l.startsWith(",") && !l.startsWith(",\"Date")))
      .map(_.split(",", -1))
      .filter(_.length >= 22)
      .flatMap(row => getPressureObservation(row))
      .filter(_.month >= 1)
      .map(obs => List(obs.month, obs.hour, obs.latitude, obs.longitude, obs.altitude,
                       obs.pressure).mkString(","))
    Util.deleteFile(s"$dataDir/$locId")
    obsRDD.saveAsTextFile(s"$dataDir/$locId")
  }

}
