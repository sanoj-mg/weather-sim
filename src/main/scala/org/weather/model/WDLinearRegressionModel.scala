package org.weather.model

import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.weather.common.Util._
/**
  * Created by sanoj on 13/8/17.
  */
object WDLinearRegressionModel {
  // Get the labeled points from data
  def getLabeledPoints(dataDir: String): RDD[LabeledPoint] = {
    val rdd = sc.textFile(s"$dataDir/*", location.size).map(_.split(","))
    val labeledPoints = rdd.map(line =>
      new LabeledPoint(
        line.last.toDouble,
        Vectors.dense(line.take(line.length - 1).toArray.map(str => str.toDouble))
      )
    )
    labeledPoints.cache
  }
  // Split features into training and test data
  def splitLabledPoints(dataPoints: RDD[LabeledPoint]): (RDD[LabeledPoint], RDD[LabeledPoint]) = {
    val dataSplit = dataPoints.randomSplit(Array(0.8, 0.2))
    val trainingSet = dataSplit(0)
    val testSet = dataSplit(1)
    (trainingSet, testSet)
  }

  // Scale features
  def scaleFeatures(dataSet: RDD[LabeledPoint], scaler: StandardScalerModel): RDD[LabeledPoint] = {
    dataSet.map(dp => new LabeledPoint(dp.label, scaler.transform(dp.features))).cache
  }
  // Train model
  def trainModel(trainingSet: RDD[LabeledPoint]): LinearRegressionModel = {
    val linearRegression = new LinearRegressionWithSGD().setIntercept(true)
    linearRegression.optimizer.setNumIterations(1000).setStepSize(0.1)
    linearRegression.run(trainingSet)
  }
  // Get the trained linear regression model
  def getTrainedModel(dataDir: String): (LinearRegressionModel, StandardScalerModel) = {
    val dataPoints = getLabeledPoints(dataDir)
    // Training set and Test Data
    val splitData = splitLabledPoints(dataPoints)
    val trainingData = splitData._1
    val testData = splitData._2
    // Scale the features
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(trainingData.map(dp => dp.features))
    val scaledTrainingSet = scaleFeatures(trainingData, scaler)
    val scaledTestSet = scaleFeatures(testData, scaler)
    // Train the model
    val trainedModel = trainModel(scaledTrainingSet)
    // Predict against Test Data
   /* val predictions: RDD[Double] = trainedModel.predict(scaledTestSet.map(point => point.features))
    predictions.take(5).foreach(println)
    predictions.map(p => "prediction: " + p).collect().foreach(println)*/
    (trainedModel, scaler)
  }
  // Get prediction for a given LinearRegressionModel & a feature vector
  def getPrediction(trainedModel: LinearRegressionModel, sampleVector: Vector,
                    scaler: StandardScalerModel): Double = {
    val scaledVector = scaler.transform(sampleVector)
    trainedModel.predict(scaledVector)
  }
}
