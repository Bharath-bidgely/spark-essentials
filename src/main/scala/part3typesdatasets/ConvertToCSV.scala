package part3typesdatasets

import org.apache.spark.sql.SparkSession
//import com.crealytics.spark.excel._

object ConvertToCSV extends App {

  val spark = SparkSession.builder()
    .appName("ConvertToCSV")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    //.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Path to the Excel file
  val excelFilePath = "/Users/bharath/Library/CloudStorage/GoogleDrive-bharath@bidgely.com/My Drive/Scala_Learning/spark-essentials/src/main/resources/data/Adhoc/ZCS_VIP.XLSX"

  // Read the Excel file
  val df = spark.read
    .format("com.crealytics.spark.excel")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(excelFilePath)

  // Write the DataFrame as a pipe-delimited CSV file
  df.write
    .option("delimiter", "|")
    .option("header", "true")
    .csv("/Users/bharath/Library/CloudStorage/GoogleDrive-bharath@bidgely.com/My Drive/Scala_Learning/spark-essentials/src/main/resources/data/Adhoc/ZCS_CSV")

}
