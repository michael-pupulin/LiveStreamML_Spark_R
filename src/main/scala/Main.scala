import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._

// Goal: Read data from Test.csv into a Spark streaming pipeline that makes a prediction
// and writes predictions to a folder. R app should show predictions of the model for each input data.


object Main {

//SPARK COMPONENT. -------------------------------------------------------------------------------

  //Spark Session and Context.
  val spark: SparkSession = SparkSession.builder()
    .appName("Classify")
    .config("spark.master","local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  //Log levels
  spark.sparkContext.setLogLevel("OFF")

  //Schema for Test Data.
  val Schema: StructType = StructType(Array(
    StructField("ID",DoubleType),
    StructField("Customer_care_calls",DoubleType),
    StructField("Customer_rating",DoubleType),
    StructField("Cost_of_the_Product",DoubleType),
    StructField("Prior_purchases",DoubleType),
    StructField("Discount_offered",DoubleType),
    StructField("Weight_in_gms",DoubleType),
    StructField("Warehouse_blockD_num",DoubleType),
    StructField("Mode_of_Shipment_num",DoubleType),
    StructField("Product_importance_num",DoubleType),
    StructField("Gender_num",DoubleType)
  ))

  // Load logistic regression model.
  val logisticRegressionModelLoaded: LogisticRegressionModel = LogisticRegressionModel.load("model")

  //Assembler creates a feature column from inputs. Required for Spark ML.
  val assembler: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("Warehouse_blockD_num","Mode_of_Shipment_num"
      ,"Product_importance_num", "Gender_num","Customer_care_calls",
      "Customer_rating","Cost_of_the_Product","Prior_purchases","Discount_offered",
      "Weight_in_gms")).setOutputCol("features")



  //SPARK STREAMING COMPONENT. -------------------------------------------------------------------------------

  // Define the stream source. This will read any csv file from the StreamInput folder into spark.
  // You can actively add files while the stream is live and they will be processed.
  val TestSource: DataFrame = spark.readStream
    .option("sep", ",")
    .option("header","true")
    .schema(Schema)
    .option("maxFilesPerTrigger", 2)
    .csv("StreamInput.csv")

  // Use assembler to create a feature column.
  val assembled: DataFrame = assembler.transform(TestSource)

  // Call the logistic regression model on each input.
  val predictions: DataFrame = logisticRegressionModelLoaded.transform(assembled).select("id","prediction")


  val Out: Unit = predictions.withColumn("time_stamp", current_timestamp())
    .writeStream
    .format("csv")
    .option("path", "StreamOutput")
    .option("header","false")
    .option("checkpointLocation", "StreamOutput")
    .outputMode("append")
    .start()
    .awaitTermination()


  def main(args: Array[String]): Unit = {
    println("Done")
  }


}