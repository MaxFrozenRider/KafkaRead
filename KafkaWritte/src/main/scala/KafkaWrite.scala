import org.apache.spark.sql.SparkSession



object KafkaWrite  extends App{

  case class Bestseller(
                         Name: String,
                         Author: String,
                         UserRating: Double,
                         Reviews: Integer,
                         Price: Integer,
                         Year: Integer,
                         Genre: String
                       )

  val spark = SparkSession.builder()
    .appName("KafkaWrite")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val dataFact = spark.read.option("header",true)
    .option("inferSchema", "true")
    .csv("src/main/resources/data/bestsellers_with_categories.csv")
    .withColumnRenamed("User Rating", "UserRating")

  val dataFactStructed = dataFact.as[Bestseller]

  dataFactStructed.show()

  dataFactStructed.toJSON
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "books")

}
