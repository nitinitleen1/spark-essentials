package Dataframes

//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataframesBasics extends App {
  //creating a spark session
  val spark = SparkSession.builder()
     .appName("Dataframes Basics")
     .config("spark.master","local")
     .getOrCreate()

  // reading a DF
  val firstDf = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")
  //showing a df
  firstDf.show()
  firstDf.printSchema()

//rows of the dataset
  firstDf.take(10).foreach(println)

//spark type
  val longType = LongType

 // schema
 val carsSchema = StructType(Array(
   StructField("Name", StringType),
   StructField("Miles_per_Gallon", DoubleType),
   StructField("Cylinders", LongType),
   StructField("Displacement", DoubleType),
   StructField("Horsepower", LongType),
   StructField("Weight_in_lbs", LongType),
   StructField("Acceleration", DoubleType),
   StructField("Year", StringType),
   StructField("Origin", StringType)
 ))

  val carDFSchema = firstDf.schema
  println(carDFSchema)

//read a dataframe with our schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carDFSchema)
    .load("src/main/resources/data/cars.json")

  //create rows by hand

  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  //create df from Tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )

  val manualCardDF = spark.createDataFrame(cars) //schema auto inferred
  manualCardDF.show()

  //create dataframe with implicit

  import spark.implicits._
  val manualCarDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")
  manualCarDFWithImplicits.printSchema()

  /**
    * Exercise Solution - Smartphone Schema and Movies.json
     */

  val smartphones = Seq(
    ("Samsung", "Galaxy S10", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0)
  )

  val smartphonesDF = smartphones.toDF("Make", "Model", "Platform", "CameraMegapixels")
  smartphonesDF.show()

val moviesDF = spark.read
  .format("json")
  .option("inferSchema","true")
  .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())
}
