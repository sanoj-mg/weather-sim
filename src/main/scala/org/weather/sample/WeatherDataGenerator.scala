package org.weather.sample

import java.util.{Calendar, Date}

import org.apache.spark.mllib.feature.StandardScalerModel
import org.weather.data.{PreprocessTemperature, WeatherDataExtractor}
import org.weather.data.PreprocessPressure
import org.weather.data.PreprocessHumidity
import org.weather.model.WDLinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.weather.common.Util._

object WeatherDataGenerator {
  /**
    * Get a list of feature objects, that will be used to generate weather samples
    */
  def generateFeature(sampleSize: Int): List[WeatherFeature] = {
    if(sampleSize <= 0) return Nil
    // Pick a location
    val locId = (1 + random.nextInt(location.size))
    // Pick a date and time
    val toTime = System.currentTimeMillis
    val cal = Calendar.getInstance
    cal.add(Calendar.YEAR, -1)
    val fromTime = cal.getTimeInMillis
    val diff = toTime - fromTime + 1
    val time = fromTime + (random.nextDouble * diff).toLong
    val date = new Date(time)
    WeatherFeature(locId, date) :: generateFeature(sampleSize - 1)
  }
  // Get predictions using the trained model and generate a weather sample
  def generateSample(tModel: LinearRegressionModel,
                     tScaler: StandardScalerModel,
                     pModel: LinearRegressionModel,
                     pScaler: StandardScalerModel,
                     hModel: LinearRegressionModel,
                     hScaler: StandardScalerModel,
                     weatherFeature: WeatherFeature): WeatherSample = {
    val temperature = WDLinearRegressionModel.getPrediction(tModel, weatherFeature.getFeatureVector(), tScaler)
    val pressure = WDLinearRegressionModel.getPrediction(pModel, weatherFeature.getFeatureVector(), pScaler)
    val humidity = WDLinearRegressionModel.getPrediction(hModel, weatherFeature.getFeatureVector(), hScaler)
    WeatherSample(weatherFeature, temperature, pressure, humidity)
  }

  def generateSamples(sampleSize: Int,
                      tModel: LinearRegressionModel, tScaler: StandardScalerModel,
                      pModel: LinearRegressionModel, pScaler: StandardScalerModel,
                      hModel: LinearRegressionModel, hScaler: StandardScalerModel
                     ): Unit = {
    // Generate features & build feature vectors for the samples
    val samples = generateFeature(sampleSize).map {
      feature => { generateSample(tModel, tScaler, pModel, pScaler, hModel, hScaler, feature ) }
    }
    // Print generated samples
    println("\n********** Generating Weather Data *****\n")
    samples.foreach(println)
    println("\n********** Done *****\n")
  }

  def main(args: Array[String]) {
    val isDownload = readLine("Download weather data from www.bom.gov.au (y/n)?")
    if(!isDownload.equalsIgnoreCase("n")) {
      // Download historic daily weather data from www.bom.gov.au
      WeatherDataExtractor.downloadAllFiles(
        startMonth = config.getProperty("weatherdata.from.month", "8").toInt,
        startYear = config.getProperty("weatherdata.from.year", "2016").toInt,
        endMonth = config.getProperty("weatherdata.to.month", "7").toInt,
        endYear = config.getProperty("weatherdata.to.year", "2017").toInt)
      println("Files have been downloaded!")
    }
    //Preprocess files, extract the required fields
    println("Parsing weather data and extracting required fields...")
    PreprocessTemperature.preprocess
    PreprocessPressure.preprocess
    PreprocessHumidity.preprocess

    // Train temperature model using linear regression
    println("\n\nTraining the temperature model using weather observations...")
    val temprDataDir = config.getProperty("process.data.temperature.directory", "weatherData/temperature")
    val tTup = WDLinearRegressionModel.getTrainedModel(temprDataDir)
    val temperatureModel = tTup._1
    val temperatureScaler = tTup._2
    println("Done.")

    // Train pressure model using linear regression
    println("\n\nTraining the pressure model using weather observations...")
    val presDataDir = config.getProperty("process.data.pressure.directory", "weatherData/pressure")
    val pTup = WDLinearRegressionModel.getTrainedModel(presDataDir)
    val pressureModel = pTup._1
    val pressureScaler = pTup._2
    println("Done.")

    // Train humidity model using linear regression
    println("\n\nTraining the humidity model using weather observations...")
    val humiDataDir = config.getProperty("process.data.humidity.directory", "weatherData/humidity")
    val hTup = WDLinearRegressionModel.getTrainedModel(humiDataDir)
    val humidityModel = hTup._1
    val humidityScaler = hTup._2
    println("Done.")

    //Generate and print weather samples
    val sampleSize = 50
    println("Generating weather samples using trained model...")
    generateSamples(sampleSize, temperatureModel, temperatureScaler,
                    pressureModel, pressureScaler, humidityModel, humidityScaler)
    println("Done")
    while(!readLine("Generate more samples?(y/n)").equalsIgnoreCase("n")) {
      generateSamples(sampleSize, temperatureModel, temperatureScaler,
        pressureModel, pressureScaler, humidityModel, humidityScaler)
    }
    sc.stop
  }
}