import Common.{FLIGHT_DATA_FILE, FLIGHT_SCHEMA}
import LoadData.loadCSVData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe.TypeTag

object Question3 {

  def main(args: Array[String]) {

    val spark = CreateSparkSession.getSparkSession()

    val flightDataDF: DataFrame = loadCSVData(FLIGHT_DATA_FILE, FLIGHT_SCHEMA, spark)

    longestRun(flightDataDF, "uk").show

    spark.stop()
  }


  def longestRun(flightDataDF: DataFrame, country: String): DataFrame = {
    val wf = Window.partitionBy("passengerId").orderBy("date")
    val maxRunUdf: UserDefinedFunction = udf((trips: List[(String, String, Int)]) => maxRun(trips, country))

    flightDataDF
      .withColumn("row_number", row_number().over(wf))
      .withColumn("from_to_order", toTuple3[String, String, Int].apply(
        col("from"),
        col("to"),
        col("row_number")))
      .groupBy("passengerId").agg(collect_list("from_to_order").as("from_to_order"))
      .withColumn("max_run", maxRunUdf(col("from_to_order")))
      .drop("from_to_order")

  }

  def maxRun(trips: List[(String, String, Int)], country: String): Int = {
    print(trips)
    val sortedTrips = trips.sortWith((x, y) => x._3 < y._3)
    var countries: Set[String] = Set()
    var max = 0
    for (trip <- sortedTrips) {
      // X -> Y
      if (!trip._1.eq(country) && !trip._2.eq(country)) {
        countries = countries + trip._1 + trip._2
        // X -> UK
      } else if (!trip._1.eq(country)) {
        countries = countries.empty
        countries += trip._1
        // UK -> X
      } else if (!trip._2.eq(country)) {
        countries = countries.empty
        countries += trip._2
        // UK -> UK
      } else {
        countries = countries.empty
      }
      if (countries.size > max) {
        max = countries.size
      }
    }
    max
  }

  def toTuple3[S: TypeTag, T: TypeTag, V: TypeTag]: UserDefinedFunction =
    udf[(S, T, V), S, T, V]((x: S, y: T, z: V) => (x, y, z))

}