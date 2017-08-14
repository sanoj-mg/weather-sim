package org.weather.data

import org.weather.common.Util
import org.weather.common.Util._

/**
  * Created by sanoj on 12/8/17.
  *
  * Preprocess downloaded whether data and extract required fields
  * Prepare and save separate observation data to train models to predict humidity
  *
  */
object PreprocessHumidity {
  // Preprocess downloaded data files
  def preprocess(): Unit = {
    val downloadDir = config.getProperty("weatherdata.save.directory", "weatherData/downloaded")
    location.keys.foreach(prepareHumidityData(downloadDir, _))
  }
  // Case class to hold humidity observation
  case class HumidityObservation(month: Double, hour: Double, latitude: Double, longitude: Double,
                                    altitude: Double, humidity: Double)
  // Prepare training data to predict humidity
  def prepareHumidityData(dirName: String, locId: Int): Unit = {
    val dataDir = config.getProperty("process.data.humidity.directory", "weatherData/humidity")
    def getHumidityObservation(row: Array[String]): List[HumidityObservation] = {
      try {
        val month = row(1).split("-")(1).toDouble
        val lat = location.get(locId).get.latitude
        val lon = location.get(locId).get.longitude
        val alt = location.get(locId).get.altitude
        val humi9am = row(11).trim.toDouble
        val humi3pm = row(17).trim.toDouble
        val obs9am = HumidityObservation(month, 9, lat, lon, alt, humi9am)
        val obs3pm = HumidityObservation(month, 15, lat, lon, alt, humi3pm)
        List(obs9am, obs3pm)
      } catch {
        case ex: Exception => List(HumidityObservation(-1, 0, 0, 0, 0, 0))
      }
    }
    val lines = sc.textFile(s"$dirName/$locId")
    val obsRDD = lines
      .filter(l => (l.startsWith(",") && !l.startsWith(",\"Date")))
      .map(_.split(",", -1))
      .filter(_.length >= 22)
      .flatMap(row => getHumidityObservation(row))
      .filter(_.month >= 1)
      .map(obs => List(obs.month, obs.hour, obs.latitude, obs.longitude, obs.altitude,
                       obs.humidity).mkString(","))
    Util.deleteFile(s"$dataDir/$locId")
    obsRDD.saveAsTextFile(s"$dataDir/$locId")
  }

}
