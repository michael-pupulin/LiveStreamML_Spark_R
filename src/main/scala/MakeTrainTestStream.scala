import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

//Goal: Read in the full csv dataset, split it into three smaller datasets and save.

object MakeTrainTestStream extends App{

  //Spark Session
  val spark: SparkSession = SparkSession.builder()
    .appName("Classify")
    .config("spark.master","local")
    .getOrCreate()

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

  //Read full csv
  val df: DataFrame = spark.read.schema(Schema).option("header", "true").csv("full.csv").orderBy(rand())

  // Show some rows. Print Schema.
  df.show(5)
  df.printSchema()
  println(s"Total number of rows = ${df.count()}")

  // Get 8000 rows for Training set.
  val Train: DataFrame = df.filter(df("ID")<=10000)
  println(s"Number of rows for training = ${Train.count()}")

  // Get 2000 rows for Test set.
  val Test: DataFrame = df.filter(df("ID")>10000)
  println(s"Number of rows for training = ${Test.count()}")

  // Get 1000 rows for Streaming set.
  val Stream: DataFrame = df.filter(df("ID")>10000 )
  println(s"Number of rows for training = ${Stream.count()}")

  // Write to csv.
  Train.repartition(1).write.option("header","true").mode(SaveMode.Overwrite).csv("Train.csv")
  Test.repartition(1).write.option("header","true").mode(SaveMode.Overwrite).csv("Test.csv")
  Stream.repartition(1).write.option("header","true").mode(SaveMode.Overwrite).csv("Stream.csv")

}
