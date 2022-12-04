import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// Goal: Read in Training and Testing dataset for machine learning. Train a logistic regression model for
// classification. Save the weights of that model for future use.

object ModelTrain extends App {

  //Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("Classify")
    .config("spark.master","local")
    .getOrCreate()

  val sc = spark.sparkContext
  //Log levels
  spark.sparkContext.setLogLevel("OFF")

  //Construct Schema
  val Schema = StructType(Array(
    StructField("ID",IntegerType),
    StructField("Warehouse_blockD",StringType),
    StructField("Mode_of_Shipment",StringType),
    StructField("Customer_care_calls",IntegerType),
    StructField("Customer_rating",IntegerType),
    StructField("Cost_of_the_Product",IntegerType),
    StructField("Prior_purchases",IntegerType),
    StructField("Product_importance",StringType),
    StructField("Gender",StringType),
    StructField("Discount_offered",IntegerType),
    StructField("Weight_in_gms",IntegerType),
    StructField("Reached.on.Time_Y.N",IntegerType)
  ))

  //String Indexer to turn categorical data into numerical.
  val indexer = new StringIndexer()
    .setInputCols(Array("Warehouse_blockD","Mode_of_Shipment","Product_importance","Gender"))
    .setOutputCols(Array("Warehouse_blockD_num","Mode_of_Shipment_num","Product_importance_num","Gender_num"))

  //Load in raw data (with strings) then convert to numerical with indexer.
  val RawTrain = spark.read.schema(Schema).option("header", "true")
    .csv("Train.csv")
  val Train = indexer.fit(RawTrain).transform(RawTrain)
    .drop("Warehouse_blockD","Mode_of_Shipment","Product_importance","Gender")

  val RawTest = spark.read.schema(Schema).option("header", "true")
    .csv("Train.csv")
  val Test = indexer.fit(RawTrain).transform(RawTrain)
    .drop("Warehouse_blockD","Mode_of_Shipment","Product_importance","Gender","Reached.on.Time_Y.N")
  Test.repartition(10).write.option("header","true").mode(SaveMode.Overwrite).csv("Test.csv")

  //Create feature columns.
  val assembler = new VectorAssembler()
    .setInputCols(Array("Warehouse_blockD_num","Mode_of_Shipment_num"
      ,"Product_importance_num", "Gender_num","Customer_care_calls",
    "Customer_rating","Cost_of_the_Product","Prior_purchases","Discount_offered",
    "Weight_in_gms")).setOutputCol("features")

  val TrainAssembled = assembler.transform(Train)
    .withColumnRenamed("Reached.on.Time_Y.N","onTime")



  //Create label column.
  val labelIndexer = new StringIndexer().setInputCol("onTime").setOutputCol("label")
  val TrainLabeled = labelIndexer.fit(TrainAssembled).transform(TrainAssembled)
  //TrainLabeled.show(5)

  //Logistic regression.
  val model = new LogisticRegression().fit(TrainLabeled)
  val predictions = model.transform(TrainLabeled)

  predictions.select ("features", "label", "prediction").show()

  // To evaluate accuracy.
  val evaluator = new BinaryClassificationEvaluator()
    .setLabelCol("label")
    .setRawPredictionCol("prediction")
    .setMetricName("areaUnderROC")

  // Print accuracy.
  val accuracy = evaluator.evaluate(predictions)
  println(accuracy)


  // Save model.
  model.write.overwrite()
    .save("model")

  // Load model.
  val logisticRegressionModelLoaded = LogisticRegressionModel.load("model")
  // Check parameters.
  println(logisticRegressionModelLoaded.coefficients)
  println(logisticRegressionModelLoaded.intercept)
}
