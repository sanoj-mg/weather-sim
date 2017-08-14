package org.weather.sample

import java.util.Date
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import java.util.Calendar
import org.weather.common.Util._

/**
  * Created by sanoj on 13/8/17.
  *
  * Case class for features required to build models
  */
case class WeatherFeature(locationId: Int, date: Date) {
  // Generate feature vector
  def getFeatureVector(): Vector = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    val month = cal.get(Calendar.MONTH) + 1
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    val loc = location.get(locationId).get
    Vectors.dense(Array(month.toDouble, hour.toDouble, loc.latitude.toDouble,
      loc.longitude.toDouble, loc.altitude.toDouble))
  }
}

