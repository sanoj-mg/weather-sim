package org.weather.sample

import org.scalatest.FunSuite
import org.weather.common.Util._
import org.weather.common.WeatherConditions.{RAIN, SUNNY}

/**
  * Created by sanoj on 14/8/17.
  */
class TestWeatherGenerator extends FunSuite {

  test("WeatherFeature.getFeatureVector should return a feature Vector") {
    val feature = WeatherFeature(1, iso8601dateFormat.parse("2017-01-14T00:30Z"))
    assert(feature.getFeatureVector().size > 0)
  }

  test("WeatherSample.estimateConditions should estimate Sunny") {
    val feature = WeatherFeature(1, iso8601dateFormat.parse("2016-10-19T00:59Z"))
    val sample = WeatherSample(feature, 17, 900, 57)
    assert(sample.estimateConditions() == SUNNY)
  }

  test("WeatherSample.estimateConditions should estimate Rain") {
    val feature = WeatherFeature(1, iso8601dateFormat.parse("2017-06-23T16:24Z"))
    val sample = WeatherSample(feature, 11, 900, 76)
    assert(sample.estimateConditions() == RAIN)
  }

  test("WeatherSample.toString should print weather data") {
    val feature = WeatherFeature(1, iso8601dateFormat.parse("2016-10-28T13:53Z"))
    val sample = WeatherSample(feature, 9.30, 913.4, 79)
    assert(sample.toString.trim.length > 0)
  }
}
