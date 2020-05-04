package Dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("DataSources and Format")
    .config("spark.master","local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))
  /*
     Reading a DF
     - format
     -schema (optional) or inferschma =true
     - zero of more option
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) //enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive(default)
    .option("path","src/main/resources/data/cars.json")
    .load()

   //alternative of reading a dataframe
  val carsDFWithOptionsMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing a DF
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   -zero of more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

 // Json Flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") //couple with schema and if spark is enable to parse the value is NULL
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") //bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV Flags
  val stocksSchema = StructType(Array(
    StructField("symbol",StringType),
    StructField("date",DateType),
    StructField("price",DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat","MMM  dd  YYYY")
    .option("header","true")
    .option("sep",",")
    .option("nullValue", "")
    .load("src/main/resources/data/stocks.csv")


  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/card.parquet")


  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  //Reading from a remote database
 val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()
  employeesDF.show()

  /*
  * Read the Movies Dataframe
  * - write as tab seperated data in a value
  * - Snappy Parquet
  * - Write it to a postgresql
   */

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .format("csv")
    .option("header","true")
    .option("sep","\t")
    .save("src/main/resources/data/movies.csv")

  moviesDF.write.save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .save()

}
