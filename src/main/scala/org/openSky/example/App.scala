package org.openSky.example

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round, sum}

import java.io.{File, FileOutputStream}
import java.util.zip.ZipInputStream
import scala.annotation.tailrec

/**
 * @author ${Jack.Song}
 *         //ORDERNUMBER	QUANTITYORDERED	PRICEEACH	ORDERLINENUMBER	SALES	ORDERDATE	STATUS	QTR_ID	MONTH_ID	YEAR_ID	PRODUCTLINE	MSRP	PRODUCTCODE	CUSTOMERNAME	PHONE	ADDRESSLINE1	ADDRESSLINE2	CITY	STATE	POSTALCODE	COUNTRY	TERRITORY	CONTACTLASTNAME	CONTACTFIRSTNAME	DEALSIZE
 *         Find the average sales amount for each year by product code. Round the average to 2 decimal places.
 *         Only consider records with status equal to 'Shipped'.
 *         The output should be written to a CSV file named output.csv with headers YEAR_ID, PRODUCTLINE, AVERAGE_SALES_AMT and be ordered by YEAR_ID and PRODUCTLINE.
 *         Provide a README.md file in your project that includes instructions on how to run the job with Docker.
 */
object App {

  def main(args: Array[String]) {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CSV Data Processing")
      .master("local[*]") // Use all available cores
      .getOrCreate()

    // Read input data
    val zipFilePath = "src/main/resources/sales_data_sample.csv.zip"
    extractCSVFromZip(zipFilePath)
    val df = spark.read
      .option("header", "true") // Use first row as header
      .csv("sales_data_sample.csv")

    val filteredDF = df.filter(col("STATUS") === "Shipped")

    // Calculate average sales amount for each year by PRODUCTLINE
    val avgSalesDF = filteredDF.groupBy("YEAR_ID", "PRODUCTLINE")
      .agg(round(sum("SALES") / sum("QUANTITYORDERED"), 2).as("AVERAGE_SALES_AMT"))

    // Order the results by YEAR_ID and PRODUCTLINE
    val orderedDF = avgSalesDF.orderBy("YEAR_ID", "PRODUCTLINE")

    //debug to be deleted
    orderedDF.show()

    // Write results to a new CSV file
    FileUtils.deleteQuietly(new File("tmpResults"))
    orderedDF
      //      .coalesce(1)  //reduce performance, pulling all data, bad practice,
      //      .repartition(1)
      .write
      .option("header", "true") // Write header row
      .csv("tmpResults")

    //merge part-uuid.csv to output.csv and delete other files generated
    mergeCSVFiles("tmpResults")

    // Stop the SparkSession
    spark.stop()
    //
  }

  def mergeCSVFiles(filePath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path(filePath)
    val destPath = new Path("result/output.csv")
    FileUtils.deleteQuietly(new File("result/output.csv"))

    val srcFile = FileUtil.listFiles(new File("tmpResults"))
      .filter(f => f.getPath.endsWith(".csv"))(0)
    //Copy the CSV file outside of Directory and rename
    FileUtil.copy(srcFile, hdfs, destPath, false, hadoopConfig)
    //Removes CRC File that create from above statement
    hdfs.delete(new Path("result/.output.csv.crc"), true)
    //Remove Directory created by df.write()
    hdfs.delete(srcPath, true)
  }

  def extractCSVFromZip(zipFilePath: String): Unit = {
    val zipInputStream = new ZipInputStream(new java.io.FileInputStream(zipFilePath))

    @tailrec
    def extractNextEntry(): Unit = {
      val entry = zipInputStream.getNextEntry
      if (entry != null) {
        if (!entry.isDirectory && entry.getName.endsWith(".csv")) {
          val fileName = entry.getName.split("/").last // Extract the file name from the path
          FileUtils.deleteQuietly(new File(fileName))
          saveCSVToFile(fileName, zipInputStream)
        }
        zipInputStream.closeEntry()
        extractNextEntry() // Recursively extract the next entry
      }
    }

    extractNextEntry()
    zipInputStream.close()
  }

  def saveCSVToFile(fileName: String, inputStream: ZipInputStream): Unit = {
    val file = new File(fileName)
    val outputStream = new FileOutputStream(file)
    val buffer = new Array[Byte](1024)

    @tailrec
    def writeEntry(): Unit = {
      val bytesRead = inputStream.read(buffer)
      if (bytesRead >= 0) {
        outputStream.write(buffer, 0, bytesRead)
        writeEntry() // Recursively write the entry to file
      }
    }

    writeEntry()
    outputStream.close()
    println(s"Saved $fileName")
  }

}
