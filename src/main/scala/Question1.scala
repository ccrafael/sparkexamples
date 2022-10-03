import Common.{FLIGHT_DATA_FILE, FLIGHT_SCHEMA}
import LoadData.loadCSVData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object Question1 {


  def main(args: Array[String]) {

    val spark = CreateSparkSession.getSparkSession()

    val flightDataDF: DataFrame = loadCSVData(FLIGHT_DATA_FILE, FLIGHT_SCHEMA, spark)

    getNumberOfFlights(flightDataDF).show

    spark.stop()
  }

  private def getNumberOfFlights(flightDataDF: DataFrame): DataFrame = {
    flightDataDF
      .withColumn("month", date_format(col("date"), "M").cast(IntegerType))
      .groupBy("month").agg(
      count("*").as("Number of flights"))
      .orderBy("month")
  }


}