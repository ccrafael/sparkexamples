import Common.{FLIGHT_DATA_FILE, FLIGHT_SCHEMA, PASSENGER_DATA_FILE, PASSENGER_SCHEMA}
import LoadData.loadCSVData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Question2 {

  def main(args: Array[String]) {

    val spark = CreateSparkSession.getSparkSession()

    val flightDataDF: DataFrame = loadCSVData(FLIGHT_DATA_FILE, FLIGHT_SCHEMA, spark)
    val passengerDataDF: DataFrame = loadCSVData(PASSENGER_DATA_FILE, PASSENGER_SCHEMA, spark)

    getMostFrequentFlyers(flightDataDF, passengerDataDF, 100).show

    spark.stop()
  }


  def getMostFrequentFlyers(flightDataDF: DataFrame, passengerDataDF: DataFrame, numFrequentFlyers: Integer): DataFrame = {
    val frequentFlyersDF: DataFrame = flightDataDF
      .groupBy(col("passengerId").as("agg_passengerId")).agg(
      count("*").as("NumberOfFlights"))
      .withColumn("row_number", row_number().over(Window.orderBy(desc("NumberOfFlights"))))
      .filter(col("row_number") < numFrequentFlyers)

    frequentFlyersDF.join(passengerDataDF,
      frequentFlyersDF("agg_passengerId") === passengerDataDF("passengerId"), "inner")
      .select(
        col("passengerId").as("Passenger ID"),
        col("NumberOfFlights").as("Number of Flights"),
        col("firstName").as("First name"),
        col("lastName").as("Last name"))
  }


}