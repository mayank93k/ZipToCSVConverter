package scala.spark.org.utility

import java.io.File
import java.time.YearMonth
import java.time.format.DateTimeFormatter
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
        }
        .maxBy(_._2) // Find the directory with the maximum (most recent) YearMonth

      mostRecentDateDir._1.getAbsolutePath // Return the absolute path of the most recent date directory
    } else {
      // Throw an exception if no valid date partitions were found
      throw new Exception("No valid date partitions found")
    }
  }

}
