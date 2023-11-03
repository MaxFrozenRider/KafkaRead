import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object KafkaRead extends App{
  val spark = SparkSession.builder()
    .appName("KafkaRead")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._


  val dataFact = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "books")
    .load()
    .selectExpr("CAST(value AS STRING)")

  import collection.JavaConverters._

  val sampleDataStr = Seq(("{\"Name\":\"10-Day Green Smoothie Cleanse\",\"Author\":\"JJ Smith\",\"UserRating\":4.7,\"Reviews\":17350,\"Price\":8,\"Year\":2016,\"Genre\":\"Non Fiction\"}"))

  val sampleData = sampleDataStr.toDF()

  val schema = schema_of_json(lit(sampleData.select($"value").as[String].first))

  dataFact
    .withColumn("value", from_json($"value", schema, Map[String, String]().asJava))
    .select(
      col("value.Name").cast(StringType).as("Name"),
      col("value.Author").cast(StringType).as("Author"),
      col("value.UserRating").cast(DoubleType).as("UserRating"),
      col("value.Reviews").cast(IntegerType).as("Reviews"),
      col("value.Price").cast(IntegerType).as("Price"),
      col("value.Year").cast(IntegerType).as("Year"),
      col("value.Genre").cast(StringType).as("Genre")
    )
    .filter("UserRating >= 4")
    .writeStream
    .format("parquet")
    .option("path", "src/main/resources/data/result1")
    .option("checkpointLocation", "src/main/resources/checkpoint")
    .start()
    .awaitTermination()

}
