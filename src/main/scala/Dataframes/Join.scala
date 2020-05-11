package Dataframes
import org.apache.spark.sql.SparkSession

object Join extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master","local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferschema","true")
    .json("src/main/resources/data/guitars.json")

  var guitaristsDF = spark.read
    .option("inferschema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferschema","true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  //inner join - default value is inner
  var guitaristbandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  //outerjoin
  //left_outer is everything in the inner table with rest of the thing with the left table, with NULLS where the data is missing
   guitaristbandsDF = guitaristsDF.join(bandsDF,joinCondition,"left_outer")
   // right_outer will have inner + right data with null on the missing data

  // full outer join is inner join with rest of all the data with null in the missing values
   guitaristsDF = guitaristsDF.join(bandsDF,joinCondition,"outer")

  // semi join i.e left_semi will only have data which is satisfying the situation and present in left table
   guitaristsDF = guitaristsDF.join(bandsDF,joinCondition,"left_semi")

  // anti join i.e results which are not satisfying the condition in the left table
   guitaristsDF = guitaristsDF.join(bandsDF,joinCondition,"left_anti").show()

  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  /**
    * Exercises
    *
    * 1. show all employees and their max salary
    * 2. show all employees who were never managers
    * 3. find the job titles of the best paid 10 employees in the company
    */



}
