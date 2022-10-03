import Common.{FLIGHT_DATA_FILE, FLIGHT_SCHEMA}
import LoadData.loadCSVData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Question4 {


  def main(args: Array[String]) {

    val spark = CreateSparkSession.getSparkSession()

    val flightDataDF: DataFrame = loadCSVData(FLIGHT_DATA_FILE, FLIGHT_SCHEMA, spark)

    getFlightsTogether(flightDataDF, 3).show

    spark.stop()
  }

  def orderIds(id1: Int, id2: Int, id3: Int): String = {
    if (id2 < id3) {
      s"${id1},${id2},${id3}"
    } else {
      s"${id1},${id3},${id2}"
    }
  }

  private def getFlightsTogether(flightDataDF: DataFrame, numFlights: Int): DataFrame = {
    val orderIdsUdf = udf((id1: Int, id2: Int, id3: Int) => orderIds(id1, id2, id3))
    flightDataDF.as("t1").join(flightDataDF.as("t2"),
      col("t1.flightId") === col("t2.flightId"), "inner")
      .filter(col("t1.passengerId") =!= col("t2.passengerId"))
      .withColumn("duplicates", orderIdsUdf(col("t1.flightId"), col("t1.passengerId"), col("t2.passengerId")))
      .dropDuplicates("duplicates")
      .groupBy(col("t1.passengerId"), col("t2.passengerId")).count()
      .filter(col("count") >= numFlights)
  }
}