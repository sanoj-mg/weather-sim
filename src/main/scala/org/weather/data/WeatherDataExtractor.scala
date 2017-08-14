package org.weather.data

import java.io.File
import java.net.URL
import org.weather.common.Util._
import scala.sys.process._

/**
  * Created by sanoj on 11/8/17.
  * Download and save the weather data file a location and for a month
  * from www.bom.gov.au
  */
object WeatherDataExtractor {
  // Download weather data for month and location
  def downloadMonthlyFile(dateStr: String, fileCode: String, locId: Int): Unit = {
    // Sample URL - http://www.bom.gov.au/climate/dwo/201701/text/IDCJDW2124.201701.csv
    val url =
      s"""
         |http://www.bom.gov.au/climate/dwo/${dateStr}/text/IDCJDW${fileCode}.${dateStr}.csv
       """.stripMargin
    try {
      println("Downloading weather data from: " + url.trim)
      val dataDir = config.getProperty("weatherdata.save.directory", "data")
      val fileName = s"${dataDir}/${locId}/${dateStr}-${locId}-${fileCode}.csv"
      println(s"Saving file: ${fileName}")
      val file = new File(fileName)
      file.getParentFile.mkdirs
      new URL(url) #> file !!
    } catch {
      case e: Exception => {
        throw new Exception(s"Error downloading/saving weather data, url: ${url}")
      }
    }
  }

  def validateMonth(month: Int) = require(month >= 1 && month <= 12, s"Invalid month $month")

  def validateYear(year: Int) = require(year >= 1900 && year <= 2999, s"Invalid year $year")

  // validate date range
  def validateMonthRange(startMonth: Int, startYear: Int, endMonth: Int, endYear:Int): Unit = {
    validateMonth(startMonth)
    validateMonth(endMonth)
    validateYear(startYear)
    validateYear(endYear)
    require(endYear > startYear || endMonth >= startYear, "Invalid date range")
  }
  // Download weather data for a given date range for all configured locations
  def downloadAllFiles(startMonth: Int, startYear: Int, endMonth: Int, endYear:Int): Unit = {
    validateMonthRange(startMonth, startYear, endMonth, endYear)
    def getNextMonth(month: Int, year: Int): (Int, Int) = {
      if(month < 12) return (month + 1, year)
      return (1, year + 1)
    }
    def downloadForALocation(startMonth: Int, startYear: Int, fileCode: String, locId: Int): Unit = {
      val dateStr = "%04d".format(startYear) + "%02d".format(startMonth)
      downloadMonthlyFile(dateStr, fileCode, locId)
      if(endYear < startYear || (endYear == startYear && endMonth <= startMonth)) return
      val nextMonth = getNextMonth(startMonth, startYear)
      downloadForALocation(startMonth = nextMonth._1, startYear = nextMonth._2, fileCode, locId)
    }
    location.values.foreach((l: Location) => downloadForALocation(startMonth, startYear, l.fileCode, l.id))
  }
}
