package Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
object ColumnsandExpressions extends App {
val spark = SparkSession.builder()
  .appName("DF and Expression")
  .config("spark.master","local")
  .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars.json")
  carsDF.show()

  //column object
  val firstColumn = carsDF.col("Name")

  //selecting a column (projection - projecting old data to new dataframe)
  val carsNameDF = carsDF.select(firstColumn)
  carsNameDF.show()

  //various select methods

  import spark.implicits._
carsDF.select(
  carsDF.col("Name"),
  col("Acceleration"),
  column("Weight_in_lbs"),
  'Year, //Scala Symbol auto converted to column
  $"Horsepower", //fancier interpolation of string , return a column object
  expr("Origin") // Expression
)
  //simple with plain column name
  carsDF.select("Name","Year")

  // -- Note either we can select the first method or the second one but not both

  // Expressions in Spark
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    carsDF.col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )

  carsWithWeightDF.show()

// select expression
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF Processing
  carsDF.withColumn("Weight_in_kgs3",col("Weight_in_lbs")/2.2)

  //renaming a column
  val cardsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs","Weight in Pounds")

  // careful with column names - if columns have space and hyphen we need to use backticks
  cardsWithColumnRenamed.selectExpr("`Weight in Pounds`")

  // remove a column
  cardsWithColumnRenamed.drop("Cylinders","Displacement")

  // filtering

  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCardDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")



  // unioning adding more rows
  val moreCardDF = spark.read
    .option("inferSchema","true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCardDF) // works if DF have same schema

  // distinct
  val allCoutriesDF = carsDF.select("Origin").distinct()
allCoutriesDF.show()



}
