import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import PreProcess._
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.RDD

object Train {
  def readData(sparkSession: SparkSession, path: String): RDD[WindInput] = {
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

    dataFrame.rdd.zipWithIndex.map { case (row: Row, index: Long) =>
      try {
        WindInput(row.getString(0) + "-" + row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), row.getDouble(6))
      } catch {
        case e: Throwable =>
          val lineNumber = index + 2 //lines start with 1 and 1 is the header
          println(s"error reading line $lineNumber")
          throw e
      }
    }
  }

  def preProcess(input: RDD[WindInput]): RDD[ProcessedWindInput] = {
    var last: Option[WindInput] = None
    input.map { windInput =>
      val output = PreProcess.process(last, windInput)
      last = Some(windInput)
      output
    }
  }

  def trainRandomForest(data: RDD[ProcessedWindInput]): RandomForestModel = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    RandomForest.trainRegressor(data.map(inputToLabeledPoint), categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }

  def trainGradientBoostedTrees(data: RDD[ProcessedWindInput]): GradientBoostedTreesModel = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32

    // Train a GradientBoostedTrees model.
    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    //intellij doet moeilijk over reassignment to val. dat is niet waar. Dit runt wel gewoon
    boostingStrategy.numIterations = 30 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 2
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val points = data.map(inputToLabeledPoint)
    GradientBoostedTrees.train(points, boostingStrategy)
  }

  def inputToLabeledPoint(w: ProcessedWindInput): LabeledPoint = {
    val vector: Vector = new DenseVector(Array(w.availability, w.direction, w.direction_offset, w.speed, w.temperature))
    new LabeledPoint(w.mwhOutput, vector)
  }

  def evaluate(model: GradientBoostedTreesModel, testData: RDD[ProcessedWindInput]): Unit = {
    val labelsAndPredictions = testData.map { input =>
      val prediction = predictMwhOutput(model, input)
      (input.mwhOutput, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case(v, p) => math.pow(v - p, 2)}.mean()
    println("Learned regression forest model:\n" + model.toDebugString)
    println("Test Mean Squared Error = " + testMSE)
  }

  def predictMwhOutput(model: GradientBoostedTreesModel, input: ProcessedWindInput): Double = {
    val point = inputToLabeledPoint(input)
    model.predict(point.features)
  }

  def createSparkSession: SparkSession = {
    val conf = new SparkConf().setMaster("local").setAppName("train")
    val sc = new SparkContext(conf)
    SparkSession.builder
      .config(conf)
      .appName("spark session example")
      .getOrCreate()
  }

}
import Train._

object TrainApp extends App {
  //  val path = "Delta_wind_forecasting/Kreekraksluis_2014_2015.csv"
  val data = readData(createSparkSession, "data/train.csv")
  val processedData = preProcess(data)

  val splits = processedData.randomSplit(Array(0.7, 0.3))
  val (trainingData, testData) = (splits(0), splits(1))

  val model = trainGradientBoostedTrees(trainingData)
  evaluate(model, testData)

  // Save and load model
  //  model.save(sc, s"target/randomForestRegressionModel${LocalDateTime.now}")
  //  val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")
}