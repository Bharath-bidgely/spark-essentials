package part3typesdatasets

package com.example.analysis

import org.apache.spark.sql.functions._
import part2dataframes.DataFrameCreator
import org.apache.spark.sql.functions._
import part2dataframes.DataFrameCreator.spark
import part2dataframes.DataFrameCreator.{df1, movies1DF}

object MovieAnalysis extends App {
  println("Starting MovieAnalysis")

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  try {
    // Access the DataFrame from DataFrameCreator
    val moviesDF = DataFrameCreator.getMoviesDF
    println(s"MoviesDF: $moviesDF")

    if (moviesDF == null) {
      println("moviesDF is null!")
    } else {
      // Perform some analysis
      val recentMovies = moviesDF.filter(col("year") > 2020)
      println("Showing recent movies:")
      recentMovies.show()

      /**
       * this is the way we can keep all DF's in one place and access them like DAO call.
       */
      movies1DF.show(2)
      df1.show(2)

    }
  } catch {
    case e: Exception =>
      println(s"An error occurred: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    // Remember to stop the SparkSession
    try {
      DataFrameCreator.spark.stop()
    } catch {
      case e: Exception => println(s"Error stopping SparkSession: ${e.getMessage}")
    }
  }



}