package scala.spark.org.utility

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import play.api.libs.json._

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.YearMonth
import java.time.format.DateTimeFormatter
import java.util.zip.ZipFile
import scala.io.Source
import scala.spark.org.common.constant.ApplicationConstant._
import scala.spark.org.common.logger.Logging
import scala.util.matching.Regex

object UtilityFunction extends Logging {
  /**
   * Finds the most recent date partition from the directories in the specified base directory.
   *
   * @param baseDir       : The base directory containing subdirectories named with date patterns.
   * @param datePattern   : Regex pattern to match date-like string in the format YYYY-MM.
   * @param dateFormatter : DateTimeFormatter to define how to parse the date strings.
   * @return The absolute path of the most recent date partition directory.
   * @throws Exception If no valid date partitions are found.
   */
  def findMostRecentDatePartition(baseDir: String, datePattern: Regex, dateFormatter: DateTimeFormatter): String = {
    logger.info(s"Started processing find the most recent date partition method for base directory: $baseDir")
    // List all directories in the specified base directory
    val directories = new File(baseDir).listFiles().filter(_.isDirectory)

    // Extract directories that match the date pattern and map them to a tuple (directory, date string)
    val dateDirs = directories.flatMap { dir =>
      datePattern.findFirstIn(dir.getName).map(dateStr => (dir, dateStr))
    }

    logger.info("Check if any valid date directories were found")
    if (dateDirs.nonEmpty) {
      val mostRecentDateDir = dateDirs
        .map { case (dir, dateStr) =>
          // Parse the date string into a YearMonth object
          (dir, YearMonth.parse(dateStr, dateFormatter))
        }.maxBy(_._2) // Find the directory with the maximum (most recent) YearMonth

      mostRecentDateDir._1.getAbsolutePath // Return the absolute path of the most recent date directory
    } else {
      // Throw an exception if no valid date partitions were found
      throw new Exception("No valid date partitions found")
    }
  }

  /**
   * Unzips all ZIP files found in a given directory.
   *
   * @param zipDir                   : The directory containing ZIP files.
   * @param extractDir               : The directory where extracted files should be placed.
   * @param extractedRecentDateValue : Most recent extracted date.
   * @return A sequence of paths to the extracted files.
   */
  def unzipFilesInDir(zipDir: String, extractDir: String, extractedRecentDateValue: Option[String]): Seq[String] = {
    logger.info("Started processing unzip file in directory method")
    val zipFiles = new File(zipDir).listFiles().filter(_.getName.endsWith(".zip"))
    zipFiles.flatMap { zipFile =>
      val extractedFiles = unzipFile(zipFile.getAbsolutePath, extractDir, extractedRecentDateValue)
      extractedFiles
    }
  }


  /**
   * Unzips a single ZIP file to the specified directory, adding a .csv extension to each extracted file.
   *
   * @param zipFilePath              :The path to the ZIP file.
   * @param extractDir               :The directory where files should be extracted.
   * @param extractedRecentDateValue : Most recent extracted date.
   * @return A sequence of paths to the extracted files.
   */
  private def unzipFile(zipFilePath: String, extractDir: String, extractedRecentDateValue: Option[String]): Seq[String] = {
    val zipFile = new ZipFile(zipFilePath)
    val entries = zipFile.entries()
    val extractedFilePaths = scala.collection.mutable.ListBuffer[String]()

    while (entries.hasMoreElements) {
      val entry = entries.nextElement()
      if (!entry.isDirectory) {
        val newFilePath = Paths.get(extractDir + "/" + extractedRecentDateValue.get + "/", entry.getName)
        Files.createDirectories(newFilePath.getParent)

        val inputStream = zipFile.getInputStream(entry)
        Files.copy(inputStream, newFilePath, StandardCopyOption.REPLACE_EXISTING)
        inputStream.close()

        extractedFilePaths += newFilePath.toString
      }
    }
    zipFile.close()
    extractedFilePaths
  }

  /**
   * Cleans up the specified temporary directory by deleting all files and subdirectories.
   *
   * @param tempDir : The path of the temporary directory to clean up.
   */
  def cleanupTempDir(tempDir: String): Unit = {
    logger.info(s"Cleaning up temp directory: $tempDir")
    val dir = new File(tempDir)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles().foreach { file =>
        if (file.isDirectory) {
          cleanupTempDir(file.getAbsolutePath) // Recursive call for subdirectories
        }
        file.delete() // Delete the file or empty directory
      }
      dir.delete() // Delete the empty directory itself
    }
    logger.info("Cleaning up of temp directory completed")
  }

  /**
   * Reads schema configuration from a JSON file.
   *
   * @param schemaConfigPath : The path to the schema configuration JSON file.
   * @return A map where the key is the schema name and the value is a tuple containing
   *         the keyword for schema identification and the StructType representing the schema.
   */
  def readSchemaConfig(schemaConfigPath: String): Map[String, (String, StructType)] = {
    logger.info(s"Reading schema configuration from a JSON file: $schemaConfigPath")
    val schemaJson = Source.fromFile(schemaConfigPath)
    val parsedJson = Json.parse(schemaJson.getLines.mkString).as[Map[String, SchemaConfig]]

    parsedJson.map { case (schemaName, config) =>
      val structType = StructType(config.fields.map { field =>
        StructField(field.name, getDataType(field.`type`), nullable = true)
      })
      (schemaName, (config.keyword, structType))
    }
  }

  /**
   * Returns the appropriate Spark SQL DataType based on the provided string.
   *
   * @param dataType : The string representation of the data type.
   * @return The corresponding DataType for Spark SQL.
   */
  private def getDataType(dataType: String): DataType = dataType match {
    case "StringType" => StringType
    case "IntegerType" => IntegerType
    case "DoubleType" => DoubleType
    // Add other data types as needed
    case _ => StringType
  }

  /**
   * This method calls the reader, transform and writer method to perform file level processing.
   *
   * @param spark    : Spark session
   * @param filePath : The path of the extracted CSV file to process.
   * @param schemas  : A map of schemas with keywords for identification.
   */
  def readTransformAndWriteFile(spark: SparkSession, filePath: String, schemas: Map[String, (String, StructType)], extractedDate: Option[String]): Unit = {
    logger.info("Read the file with the appropriate schema")
    val (parsedDataFrame, keyword) = readCsvWithSchema(spark, filePath, schemas)

    logger.info("Transform the DataFrame")
    val transformedDf = transformFiles(spark, parsedDataFrame)

    writeCsvFile(transformedDf, keyword, extractedDate)
  }

  /**
   * Moves a file from a temporary location to an actual location in HDFS.
   *
   * @param tempOutputDir   The path of the temporary file.
   * @param finalOutputFile The destination path where the file will be moved.
   */
  private def mergePartFiles(tempOutputDir: String, finalOutputFile: String): Unit = {
    val tempDir = new File(tempOutputDir)

    // Ensure the temp directory exists
    require(tempDir.exists() && tempDir.isDirectory, s"Temp directory $tempOutputDir does not exist or is not a directory")

    // Find all part files in the temp directory and sort them to maintain order
    val partFiles = tempDir.listFiles().filter(_.getName.startsWith("part-")).sortBy(_.getName)

    // Create the output file and its parent directories if they don’t exist
    val finalFile = new File(finalOutputFile)
    finalFile.getParentFile.mkdirs()

    // Create a writer for the final output file
    val outputWriter = new BufferedWriter(new FileWriter(finalFile))

    try {
      partFiles.zipWithIndex.foreach { case (partFile, index) =>
        val source = Source.fromFile(partFile)

        try {
          val lines = source.getLines()

          // Write header only for the first file
          if (index == 0 && lines.hasNext) {
            outputWriter.write(lines.next()) // Write header line
            outputWriter.newLine()
          } else if (lines.hasNext) {
            lines.next() // Skip header line in subsequent files
          }

          // Write the remaining lines of each part file
          lines.foreach { line =>
            outputWriter.write(line)
            outputWriter.newLine()
          }
        } finally {
          source.close()
        }
      }
    } finally {
      outputWriter.close()
    }
  }

  /**
   * Read an extracted CSV file based on its schema
   *
   * @param spark    : Spark Session
   * @param filePath : The path of the extracted CSV file to process.
   * @param schemas  : A map of schemas with keywords for identification.
   * @return
   */
  private def readCsvWithSchema(spark: SparkSession, filePath: String, schemas: Map[String, (String, StructType)]): (DataFrame, String) = {
    logger.info("Identify the schema to use based on the file name")
    val (keyword, schema) = schemas.find { case (_, (keyword, _)) =>
      filePath.contains(keyword)
    }.map(_._2).getOrElse(throw new RuntimeException(s"No matching schema for file: $filePath"))

    logger.info(s"Reading file with selected schema: $filePath and file name is: $keyword")

    val readDataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .csv(filePath)

    (readDataFrame, keyword)
  }

  /**
   * Transform the data on the basis of each type of file.
   *
   * @param spark     : Spark Session
   * @param dataFrame : Input dataframe
   * @return Transformed output dataframe
   */
  private def transformFiles(spark: SparkSession, dataFrame: DataFrame): DataFrame = {
    logger.info("Transform the data on the basis of each type of file")
    dataFrame.columns.foldLeft(dataFrame)((acc, colName) => acc.withColumnRenamed(colName, colName.toLowerCase))
  }

  /**
   * Writes the transformed data to an output directory.
   *
   * @param dataFrame : Transformed dataframe
   * @param keyword   : Keyword for schema identification
   */
  private def writeCsvFile(dataFrame: DataFrame, keyword: String, extractedDate: Option[String]): Unit = {
    val tempOutputDir = s"$TempOutputDir/temp_$keyword"
    logger.info(s"Temporary directory for spark output: $tempOutputDir")

    logger.info("Write the transformed DataFrame to a separate CSV file")
    dataFrame.write.mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "|")
      .csv(tempOutputDir)

    val finalOutputFile = s"$OutputCsvPath/${extractedDate.get}/$keyword.csv"

    // Merge the part files into a single CSV file
    mergePartFiles(tempOutputDir, finalOutputFile)

    // Clean up the temporary directory
    cleanupTempDir(tempOutputDir)

    logger.info(s"Processed and saved file: $finalOutputFile")
  }

  // Define case classes to map JSON structure
  case class Field(name: String, `type`: String)

  case class SchemaConfig(keyword: String, fields: List[Field])

  object SchemaConfig {
    implicit val fieldReads: Reads[Field] = Json.reads[Field]
    implicit val schemaConfigReads: Reads[SchemaConfig] = Json.reads[SchemaConfig]
  }
}
