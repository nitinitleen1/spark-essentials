package Dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Aggregation extends App {
 val spark = SparkSession.builder()
   .appName("Aggregations and Grouping")
   .config("spark.master","local")
   .getOrCreate()

val moviesDF = spark.read
  .option("inferSchema","true")
  .json("src/main/resources/data/movies.json")

  // counting
  moviesDF.show()


  val genreCountDF = moviesDF.select(count( col("Major_Genre"))) //all the values execpt Null
  genreCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count("*")).show() // all the rown with Null
  moviesDF.select(countDistinct("Major_Genre")).show()
  moviesDF.select(approx_count_distinct("Major_Genre")).show()

  // min and max
  moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // average
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from moviesDF groupby "Major_Genre"

  val averageIMDBbyGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
    
  val aggregationByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

  aggregationByGenre.show()


  /**
  * Exercise
    * All the movies and all its profit
    * Count how many distinct directors we have
    * Show the mean and standard deviation of US Gross Movies Sales
    * Compute the average IMDB Rating and AVg Gross Per Director
  */
//1
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross")+ col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()
  moviesDF.select(countDistinct(col("Director"))).show()
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))).show()


  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_rating"),
      sum("US_Gross").as("Total_Gross")
    )
    .orderBy(col("Avg_rating").desc_nulls_last).show()
}
