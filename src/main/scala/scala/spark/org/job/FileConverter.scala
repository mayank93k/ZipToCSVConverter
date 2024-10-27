package scala.spark.org.job

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.YearMonth
import java.time.format.DateTimeFormatter
import scala.spark.org.common.constant.ApplicationConstant._
import scala.spark.org.common.logger.Logging
import scala.spark.org.utility.UtilityFunction.{findMostRecentDatePartition, unzipFilesInDir}

object FileConverter extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("Convert ZIP file into CSV format process")

    val spark = SparkSession.builder().appName("Convert Zip file to csv format").master("local").getOrCreate()

    // Regex pattern to match date-like strings in the format YYYY-MM
    val datePattern = RegexDatePattern.r

    // DateTimeFormatter to define how to parse the date strings
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM")

    // Implicit ordering for Year Month to allow comparison for finding the most recent date
    implicit val yearMonthOrdering: Ordering[YearMonth] = Ordering.by(_.atDay(1).toEpochDay)

    // Find the most recent date partition and unzip all the files in the directory.
    val mostRecentDateDir = findMostRecentDatePartition(ZipFileBaseDir, datePattern, dateFormatter)
    val extractedFilePath = unzipFilesInDir(mostRecentDateDir, ExtractCsvFileDir)

    // Process the extracted CSV files.
    val resultDF = processCsvFiles(spark, extractedFilePath, OutputCsvPath)

    logger.info(s"Write the transformed dataframe to a CSV file in the path: $OutputCsvPath")
    resultDF.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").option("delimiter", ",").csv(OutputCsvPath)

    spark.stop()
  }

  /**
   * Reads, transforms, and writes the extracted CSV files using Spark.
   *
   * @param spark              : Spark Session
   * @param extractedFilePaths :Paths to the extracted CSV files.
   * @param outputCsvPath      :Path where the transformed CSV should be saved.
   */
  private def processCsvFiles(spark: SparkSession, extractedFilePaths: Seq[String], outputCsvPath: String): DataFrame = {
    val columnNames = Seq("code", "description") // Define the column names for the DataFrame.
    val schema = StructType(columnNames.map(fieldName => StructField(fieldName, StringType, nullable = true)))

    logger.info("Read and transform each extracted CSV file.")
    val df = spark.read
      .option("header", "false")
      .option("delimiter", ";")
      .schema(schema)
      .csv(extractedFilePaths: _*)

    // Perform transformations (e.g., make column names lowercase).
    val transformedDf = df.columns.foldLeft(df)((acc, colName) => acc.withColumnRenamed(colName, colName.toLowerCase))

    transformedDf
  }

}
