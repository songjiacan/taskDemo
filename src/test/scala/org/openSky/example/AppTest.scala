package org.openSky.example

/*
Here's an example of a FunSuite with Matchers mixed in:
*/
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.specs2.runner.JUnitRunner
@RunWith(classOf[JUnitRunner])
class AppTest extends AnyFunSuite with BeforeAndAfterAll {

  // Initialize SparkSession
  private val spark: SparkSession = SparkSession.builder()
    .appName("CSV Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  private val df: DataFrame = App.generateDataFrameFromInputCSVFile(spark, "src/test/resources/sales_data_sample_test.csv")

  test("Test CSV file for row with 'AVERAGE_SALES_AMT' column equal to 3570.18") {
    // Filter DataFrame to find rows where 'AVERAGE_SALES_AMT' column is equal to 3570.18
    val yearIdDF = df.filter($"YEAR_ID" === 2003)
    val productLineDF = df.filter($"PRODUCTLINE".contains("Motorcycles"))
    val filteredDF = df.filter($"AVERAGE_SALES_AMT" === 3570.18)

    // Check if there is at least one row in the DataFrame
    assert(df.count() == 1, "CSV file total row is 1")
    assert(yearIdDF.count() == 1, "CSV file contain row with 'YEAR_ID' column equal to 2003")
    assert(productLineDF.count() == 1, "CSV file contain row with 'PRODUCTLINE' column equal to 'Classic Cars'")
    assert(filteredDF.count() == 1, "CSV file does contain row with 'AVERAGE_SALES_AMT' column equal to 3570.18")
  }

  override protected def afterAll(): Unit = {
    // Stop SparkSession after test suite execution
    spark.stop()
    super.afterAll()
  }
}