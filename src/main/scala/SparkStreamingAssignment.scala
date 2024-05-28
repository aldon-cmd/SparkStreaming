import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkStreamingAssignment {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .master("local[*]")
        .getOrCreate()

      // Set the log level to only print errors
      spark.sparkContext.setLogLevel("ERROR")

      // Streaming source that monitors the data/logs directory for text files
      val accessLines = spark.readStream.text("dataS/logs")

      // Regular expressions to extract pieces of Apache access log lines
      val contentSizeExp = "\\s(\\d+)$"
      val statusExp = "\\s(\\d{3})\\s"
      val generalExp = "\"(\\S+)\\s(\\S+)\\s*(\\S*)\""
      val timeExp = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} -\\d{4})]"
      val hostExp = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"
      val siteExp = "\"(\\S+)\""

      // Apply these regular expressions to create structure from the unstructured text
      val logsDF = accessLines.select(regexp_extract(col("value"), hostExp, 1).alias("host"),
          regexp_extract(col("value"), siteExp, 1).alias("site"),
          regexp_extract(col("value"), timeExp, 1).alias("timestamp"),
          regexp_extract(col("value"), generalExp, 1).alias("method"),
          regexp_extract(col("value"), generalExp, 2).alias("endpoint"),
          regexp_extract(col("value"), generalExp, 3).alias("protocol"),
          regexp_extract(col("value"), statusExp, 1).cast("Integer").alias("status"),
          regexp_extract(col("value"), contentSizeExp, 1).cast("Integer").alias("content_size"))



      //1) What is the total number of request per site?
      val hostCountsDF = logsDF.groupBy("site").count()
      val query2 = hostCountsDF.writeStream.outputMode("complete").format("console").queryName("query2").start()


      // 2) For each site, what is the breakdown of each http response code?
      val statusCountsDF = logsDF.groupBy("site", "status").count()
      val query = statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start()


      //3) Which path and method is resulting in the most 500/404 error codes per site?
      val errorCountsDF = logsDF.filter(col("status") rlike "500|404").groupBy("endpoint","method","status")
        .count()
        .select("endpoint","method","count")
      val query3 = errorCountsDF.writeStream.outputMode("complete").format("console").queryName("query3").start()
      // Wait until we terminate the scripts

      query2.awaitTermination()
      query.awaitTermination()
      query3.awaitTermination()

      // Stop the session
      spark.stop()
  }
}
