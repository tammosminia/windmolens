import java.time.LocalDateTime
import java.util.Date

import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Train extends App {
  val conf = new SparkConf().setMaster("local").setAppName("train")
  val sc = new SparkContext(conf)

  val sparkSession = SparkSession.builder
    .config(conf)
    .appName("spark session example")
    .getOrCreate()

//  val path = "Delta_wind_forecasting/Kreekraksluis_2014_2015.csv"
  val path = "data/train.csv"
  val dataFrame = sparkSession.read
    .option("header", "true")
      .schema(StructType(Array(
        StructField("datum", StringType),
        StructField("tijd", StringType),
        StructField("MW/H out", DoubleType),
        StructField("beschikbaarheid", DoubleType),
        StructField("windsnelheid", DoubleType),
        StructField("windrichting", DoubleType),
        StructField("EELDE_MC_TEMPERATURE", DoubleType),
        StructField("EELDE_MC_WINDSPEED", DoubleType),
        StructField("EELDE_MC_WINDDIRECTION", DoubleType),
        StructField("EELDE_MC_CLOUDCOVER", DoubleType),
        StructField("EELDE_MC_PRESSURE", DoubleType),
        StructField("EELDE_MC_RADIATION", DoubleType)
      )))
    .csv(path)

  // Load and parse the data file.
//  val data = MLUtils.loadLibSVMFile(sc, "data/train.csv")

  val data = dataFrame.rdd.zipWithIndex.map { case (row: Row, index: Long) =>
    val lineNumber = index + 2 //lines start with 1 and 1 is the header
    assert(!row.isNullAt(2), s"line $lineNumber $row is not ok")
    val vector: Vector = new DenseVector(Array(row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6), row.getDouble(7), row.getDouble(8), row.getDouble(9), row.getDouble(10), row.getDouble(11)))
    new LabeledPoint(row.getDouble(2), vector)
  }

  // Split the data into training and test sets (30% held out for testing)
  val splits = data.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  // Train a RandomForest model.
  // Empty categoricalFeaturesInfo indicates all features are continuous.
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 3 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 4
  val maxBins = 32

  val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  // Evaluate model on test instances and compute test error
  val labelsAndPredictions = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow((v - p), 2)}.mean()
  println("Learned regression forest model:\n" + model.toDebugString)
  println("Test Mean Squared Error = " + testMSE)

  // Save and load model
//  model.save(sc, s"target/randomForestRegressionModel${LocalDateTime.now}")
//  val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")

}
