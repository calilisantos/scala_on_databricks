// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Welcome to request_with_scala
// MAGIC This project has educational goals and aims to explore how to use Scala in Databricks, consuming an API, and dealing with the response using Spark and native solutions.
// MAGIC
// MAGIC ## The source:
// MAGIC The Reddit APIs were used. It returns the top 50 stocks discussed in the Wallstreetbets subreddit over the last 15 minutes, including a sentiment analysis of the discussions. Documentation is available [here](https://tradestie.com/apps/reddit/api/).
// MAGIC
// MAGIC ## Cluster Configs:
// MAGIC
// MAGIC For this project, DBR 13.3 was used*, and the current ENV was set to use JDK-11 on the cluster:
// MAGIC
// MAGIC `JNAME=zulu11-ca-amd64` 
// MAGIC
// MAGIC This JVM is necessary to enable `java.net.http.HttpRequest`** in Databricks and is required by the request libraries described below:
// MAGIC
// MAGIC | Scala package | Description | Maven coordinates | Reference |
// MAGIC | - | - | - | - |
// MAGIC | **sttp.client3** | Scala library that provides HTTP request and response handlers. |`com.softwaremill.sttp.model:core_2.12:1.7.10`| [Documentation](https://sttp.softwaremill.com/en/stable/simple_sync.html) |
// MAGIC | **sttp.model** | Provides HTTP models such as headers, URIs, methods, etc. Required for `sttp.client`.|`com.softwaremill.sttp.tapir:tapir-sttp-client_2.12:1.10.6`| [Documentation](https://sttp.softwaremill.com/en/stable/model/model.html) |
// MAGIC
// MAGIC *Between DBR 11.3 and 14.3 is not expected incompatibility.
// MAGIC
// MAGIC **error found: `BootstrapMethodError: java.lang.NoClassDefFoundError: java/net/http/HttpRequest`. Solution find [here](com.softwaremill.sttp.tapir:tapir-sttp-client_2.12:1.10.6).

// COMMAND ----------

// MAGIC %md
// MAGIC # Dependencies import

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_timestamp, lit, when}
import sttp.client3.{basicRequest, Response, SimpleHttpClient}
import sttp.model.{StatusCode, Uri}

// COMMAND ----------

// MAGIC %md
// MAGIC # Define ReadRequest
// MAGIC A class to deal with sttp.client Response, with attributes:
// MAGIC * `client`: A **SimpleHttpClient** from **_sttp.client_** instance. Used to execute the request.
// MAGIC * `requestEndpoint`: The endpoint informed
// MAGIC * `successStatusCode`: The 200 status code
// MAGIC
// MAGIC And the methods:
// MAGIC * `checkRequestStatusCode`: Raises a exception if response status code is different from 200.
// MAGIC * `transformResponseToDataframe`: Return a spark dataframe if request was succesfull.

// COMMAND ----------

class ReadRequest(endpoint: Uri) {
  val client = SimpleHttpClient()
  val requestEndpoint: Uri = endpoint
  val successStatusCode: String = "200"

  def getResponse: Response[Either[String, String]] = {
    client.send(
      basicRequest.get(requestEndpoint)
    )
  }

  def checkRequestStatusCode(responseStatusCode: StatusCode) {
    if (responseStatusCode.toString != successStatusCode) {
      throw new Exception(s"Status code from request is different of $successStatusCode. Check the url")
    }
  }

  def transformResponseToDataframe(responseBody: Either[String, String]): DataFrame = {
    responseBody match {
      case Right(jsonString) =>
        spark.read.json(
          Seq(jsonString).toDS()
        )
      case Left(errorMessage) =>
        spark.emptyDataFrame
    }
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Request param
// MAGIC * `redditEndpoint`: The API endpoint in **_sttp.model_** **Uri** instance.

// COMMAND ----------

// val redditEndpoint: Uri = uri"https://tradestie.com/api/v1/apps/reddit"
val redditEndpoint: Uri = Uri.parse("https://tradestie.com/api/v1/apps/reddit")
  .getOrElse(throw new Exception("Invalid URI"))

// COMMAND ----------

// MAGIC %md
// MAGIC # ETL params

// COMMAND ----------

val accurTable: String = ".ac_sentiment_analysis"
val databaseName: String = "db_stocks"
val rawTable: String = ".rw_sentiment_analysis"
val goodFeelling: String = "Bullish"
val partitionColumn: String = "partition_num"

// COMMAND ----------

// MAGIC %md
// MAGIC # Getting API response

// COMMAND ----------

val readRequest = new ReadRequest(redditEndpoint)

val response = readRequest.getResponse

readRequest.checkRequestStatusCode(response.code)

val data: DataFrame = readRequest.transformResponseToDataframe(response.body)

display(data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Creating database

// COMMAND ----------

spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

// COMMAND ----------

// MAGIC %md
// MAGIC # Creating raw table

// COMMAND ----------

data.withColumn(partitionColumn, current_timestamp()).write
  .mode("overwrite")
    .format("delta")
      .option("header", "true")
        .saveAsTable(databaseName.concat(rawTable))

// COMMAND ----------

// MAGIC %md
// MAGIC # Adding a_good_feelling column

// COMMAND ----------

val accur_data = data.withColumn(
  "a_good_feelling",
  when(col("sentiment") === goodFeelling, lit(true))
    .otherwise(lit(false))
)
display(accur_data)

// COMMAND ----------

// MAGIC %md
// MAGIC # Creating accur table

// COMMAND ----------

accur_data.withColumn(partitionColumn, current_timestamp()).write
  .mode("overwrite")
    .format("delta")
      .option("header", "true")
        .saveAsTable(databaseName.concat(accurTable))

// COMMAND ----------

// MAGIC %md
// MAGIC # Querying raw table

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $databaseName$rawTable"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Querying accur table

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $databaseName$accurTable"))
