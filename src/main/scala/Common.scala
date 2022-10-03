import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object Common {
  val PASSENGER_SCHEMA: StructType = new StructType()
    .add("passengerId", IntegerType)
    .add("firstName", StringType)
    .add("lastName", StringType)

  val FLIGHT_SCHEMA: StructType = new StructType()
    .add("passengerId", IntegerType)
    .add("flightId", IntegerType)
    .add("from", StringType)
    .add("to", StringType)
    .add("date", DateType)

  val FLIGHT_DATA_FILE = "data/flightData.csv.gz"
  val PASSENGER_DATA_FILE = "data/passengers.csv.gz"
}
