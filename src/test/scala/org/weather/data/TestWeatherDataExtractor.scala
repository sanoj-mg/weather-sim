package org.weather.data

import java.nio.file.{Files, Paths}

import org.scalatest.FunSuite
import org.weather.common.Util.config

/**
  * Created by sanoj on 14/8/17.
  */
class TestWeatherDataExtractor extends FunSuite {

  test("WeatherDataExtractor.downloadMonthlyFile should download a weather data file from www.bom.gov.au") {
    WeatherDataExtractor.downloadMonthlyFile("201701", "2124", 999)
    val dataDir = config.getProperty("weatherdata.save.directory", "data")
    val fileName = s"${dataDir}/999/201701-999-2124.csv"
    assert(Files.exists(Paths.get(fileName)))
    Files.delete(Paths.get(fileName))
  }

  test("WeatherDataExtractor.validateMonth should throw exception for invalid month") {
    val thrown = intercept[IllegalArgumentException] {
      WeatherDataExtractor.validateMonth(15)
    }
    assert(thrown.getMessage == "requirement failed: Invalid month 15")
  }

  test("WeatherDataExtractor.validateMonth should not throw any exception for valid month") {
    WeatherDataExtractor.validateMonth(4)
  }

  test("WeatherDataExtractor.validateYear should throw exception for invalid year") {
    val thrown = intercept[IllegalArgumentException] {
      WeatherDataExtractor.validateYear(99999)
    }
    assert(thrown.getMessage == "requirement failed: Invalid year 99999")
  }

  test("WeatherDataExtractor.validateYear should not throw any exception for valid year") {
    WeatherDataExtractor.validateYear(2016)
  }

  test("WeatherDataExtractor.validateMonthRange should throw exception for invalid date range") {
    val thrown = intercept[IllegalArgumentException] {
      WeatherDataExtractor.validateMonthRange(4, 2017, 9, 2016)
    }
    assert(thrown.getMessage == "requirement failed: Invalid date range")
  }

  test("WeatherDataExtractor.validateMonthRange should not throw any exception for valid date range") {
    WeatherDataExtractor.validateMonthRange(10, 2016, 3, 2017)
  }
}
