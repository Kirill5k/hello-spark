import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}


object PoModelTest extends App {
  final case class FactPoModelScore(
                                     device_id: String,
                                     channel_id: Int,
                                     percentile: Int,
                                     prob: Double,
                                     action: Int
                                   )

  final case class ScoresByAccount(
                                    account_id: String,
                                    scores: List[FactPoModelScore]
                                  )

  val spark = SparkSession.builder()
    .appName("po_model")
    .config("spark.master", "local[*]")
    .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
    .getOrCreate()

  import spark.implicits._

  // Define encoders
//  implicit val factPoModelScoreEncoder: Encoder[FactPoModelScore] = Encoders.product[FactPoModelScore]
//  implicit val scoresByAccountEncoder: Encoder[ScoresByAccount] = Encoders.product[ScoresByAccount]

  val sqlQuery = s"""
    SELECT
      account_id,
      collect_list(named_struct(
        'device_id', device_id,
        'channel_id', channel_id,
        'percentile', percentile,
        'prob', prob,
        'action', action
      )) AS scores
    FROM parquet_table
    WHERE date_key = '2024-08-21'
    GROUP BY account_id
  """

  val parquetDF = spark.read.parquet("data/po_scores")
  parquetDF.createOrReplaceTempView("parquet_table")

  val df = spark.sql(sqlQuery)

  // Convert the DataFrame into Dataset[ScoresByAccount]
  val scoresByAccountDS: Dataset[ScoresByAccount] = df.as[ScoresByAccount]

  // Show the result
  scoresByAccountDS.show(false)
}
