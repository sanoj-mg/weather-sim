package org.weather.common

import java.io.File
import java.nio.file.{Files, Paths}
import WeatherConditions._

import org.scalatest.FunSuite

/**
  * Created by sanoj on 14/8/17.
  */
class TestCommon extends FunSuite  {

  test("Util.using should close a resource after usage")  {
    object Resource{
      var closed: Boolean = false
      def close(): Unit = closed = true
    }
    Util.using(Resource)(x => x)
    assert(Resource.closed)
  }

  test("Util.deleteFile should delete a file from local file system") {
    val filePath = "TestDeleteFile_Util.txt"
    new File(filePath).createNewFile()
    Util.deleteFile(filePath)
    assert(!Files.exists(Paths.get(filePath)))
  }

  test("Util.location should load predefined locations") {
    assert(Util.location.size > 0)
  }

  test("Util.readLocatons should read predefined locations") {
    assert(Util.readLocatons("/location.txt").size > 0)
  }

  test("Util.config should load config properties") {
    assert(Util.config.size > 0)
  }

  test("WeatherConditions.toString should print correct name") {
    assert(SNOW.toString == "Snow")
  }

}
