package org.sparkchallenge2


import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

object SparkChallengeExercise {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder()
      .appName("XpandITchallenge")
      .master("local[1]")
      .getOrCreate()

    val filePath = "C:\\Users\\dagui\\SparkChallenge\\google-play-store-apps\\googleplaystore_user_reviews.csv"
    val filePath2 = "C:\\Users\\dagui\\SparkChallenge\\google-play-store-apps\\googleplaystore.csv"


    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val df1: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath2)

    //PART 1 ----------------------------------------------------------------------------------

    val averageSentimentDF: DataFrame = df.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    val finalDF: DataFrame = averageSentimentDF.select(
      col("App"),
      col("Average_Sentiment_Polarity").cast("double").as("Average_Sentiment_Polarity")
    )
    val df_1: DataFrame = finalDF
      .na.fill(0, Seq("Average_Sentiment_Polarity"))

    df_1.printSchema()
    df_1.show()

    //PART 2 ---------------------------------------------------------------------------------

    val filterDF: DataFrame = df1.filter("Rating >= 4").sort(col("Rating").desc)

    filterDF.printSchema()
    filterDF.show()

    val outputFilePath = "C:\\Users\\dagui\\SparkChallenge\\best_apps.csv"

    filterDF.write
      .option("header", "true")
      .option("delimiter", "§")
      .csv(outputFilePath)

    //PART3 ------------------------------------------------------------------------------------------
    val groupedDF: RelationalGroupedDataset = df1.groupBy("App")
    val ArrayCatDF: DataFrame = groupedDF.agg(collect_set("Category").as("Categories"))
    val maxReviewsDF: DataFrame = groupedDF.agg(max("Reviews").as("MaxReviews"))
    val joinDF: DataFrame = ArrayCatDF.join(maxReviewsDF, Seq("App"))
    val finalDF2: DataFrame = joinDF.join(df1, Seq("App"))
    val df_3: DataFrame = finalDF2.select(df1.columns.map(col): _*).distinct()

    df_3.printSchema()
    df_3.show()

  //PART4---------------------------------------------------------------------------------

    val finalDF3: DataFrame = df_1.join(df_3, Seq("App"), "inner")
    val outPath = "C:\\Users\\dagui\\SparkChallenge\\googleplaystore_cleaned"

    finalDF3.printSchema()
    finalDF3.show()

    finalDF3.write
      .option("compression", "gzip")
      .parquet(outPath)

  //PART5-------------------------------------------------------------------------------------------

    val finalDF5: DataFrame = df_3.groupBy("Genres")
      .agg(
        count("App").as("Number_of_Applications"),
        avg("Rating").as("Average_Rating")
      )

    val averageSentimentDF2: DataFrame = finalDF3.groupBy("Genres")
      .agg(avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

    val df_4: DataFrame = finalDF5.join(averageSentimentDF2, Seq("Genres"))

    df_4.printSchema()
    df_4.show()

    val outPath2 = "C:\\Users\\dagui\\SparkChallenge\\googleplaystore_metrics"

    df_4.write
      .option("compression", "gzip")
      .parquet(outPath)



    spark.stop()
  }
}