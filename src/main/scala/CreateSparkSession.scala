import org.apache.spark.sql.SparkSession

object CreateSparkSession {
  def getSparkSession(): SparkSession = {
    val spark = SparkSession.builder
      .appName("Example")
      .master("local[4]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    spark
  }

}
