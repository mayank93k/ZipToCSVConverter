package scala.spark.org.job

import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter
import scala.spark.org.common.constant.ApplicationConstant._
import scala.spark.org.common.logger.Logging
import scala.spark.org.utility.UtilityFunction._

object FileConverter extends Logging {
  def main(args: Array[String]): Unit = {
    logger.info("Convert ZIP file into CSV format process")
    val spark = SparkSession.builder().appName("Convert Zip file to csv format").master("local").getOrCreate()

    // Regex pattern to match date-like strings in the format YYYY-MM
    val datePattern = RegexDatePattern.r

    // DateTimeFormatter to define how to parse the date strings
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM")

    try {
      // Load schemas from the JSON configuration.
      val schemasRead = readSchemaConfig(schemaConfigPath)

      // Find the most recent date partition and unzip all the files in the directory.
      val findMostRecentDateDir = findMostRecentDatePartition(ZipFileBaseDir, datePattern, dateFormatter)
      val extractedFilePaths = unzipFilesInDir(findMostRecentDateDir, ExtractCsvFileDir)

      logger.info("Started processing each extracted file based on its schema")
      extractedFilePaths.foreach(filePath => readTransformAndWriteFile(spark, filePath, schemasRead))
    } finally {
      logger.info("Cleanup the temporary directory after processing")
      cleanupTempDir(TempOutputDir)
      spark.stop()
    }
  }

}
