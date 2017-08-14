package org.weather.sample

import org.weather.common.Util._
import java.util.{Calendar, Date}
import org.weather.common.WeatherConditions._

/**
  * Created by sanoj on 13/8/17.
  *
  * Case class that represents a weather sample generated using the trained models
  */
case class WeatherSample(feature: WeatherFeature, temperature: Double,
                         pressure: Double, humidity: Double) {
  // Method to print weather sample in required format
  override def toString: String = {
    val loc = location.get(feature.locationId).get
    // Location
    val locStr = loc.name
    // Position
    val pos = s"${loc.latitude},${loc.longitude}"
    // Local time
    val localTime = iso8601dateFormat.format(feature.date)
    // Conditions
    val cond = estimateConditions
    val tempStr = "%3.2f".format(temperature).trim
    val pressureStr = "%5.1f".format(pressure).trim
    val humidityStr = "%3.0f".format(humidity).trim

    s"${locStr}|${pos}|${localTime}|${cond.toString}|${tempStr}|${pressureStr}|${humidityStr}"
  }

  // Estimate conditions(Sunny,Rain,Snow) from hour of day, temperature and humidity
  // TODO -- Build a model to estimate weather conditions from rainfall & sunshine
  def estimateConditions(): Condition = {
    val cal = Calendar.getInstance()
    cal.setTime(feature.date)
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    if(temperature <= 7 && humidity >= 80) {
      return SNOW
    } else if(temperature > 7 && temperature <= 11 && humidity >= 70) {
      return RAIN
    } else {
      if(hour <= 6 || hour >= 18) { // night time, can not be sunny
        return RAIN
      } else {
        return SUNNY
      }
    }
  }
}

