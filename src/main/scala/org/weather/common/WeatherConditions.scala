package org.weather.common

/**
  * Created by sanoj on 13/8/17
  */
object WeatherConditions {

  sealed abstract class Condition(id: Int, name: String) {
    override def toString = name
  }
  case object RAIN extends Condition(1, "Rain")
  case object SNOW extends Condition(2, "Snow")
  case object SUNNY extends Condition(3, "Sunny")
}
