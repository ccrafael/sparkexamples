import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object LoadData {
  def loadCSVData(passengerFileData: String, schema: StructType, spark: SparkSession) = {
    spark.read.format("csv")
      .option("header", "true")
      .option("compression", "gzip")
      .schema(schema)
      .load(passengerFileData)
  }
}
