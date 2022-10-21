
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.sprk.sql._
import org.apache.sql.tyoes._
import org.apache.spark.sql.function._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType

object steam_processing_app {
  def main(args: Array[String]) : Unit = {
    println("Stram processing Application Started..")

    val Kafka_topic_name = "meetuprsvptopic"
    val kafka_bootstrap_servers = "localhost:9092"

    val myssq_host_name = "localhost"
    val mysql_port_no = "3306"
    val mysql_user_name = "root"
    val mysql_password = "sushant"
    val mysql_database_name = "meetup_rsvp_db"
    val mysql_driver_class = "com.mysql.jdbc.Driver"
    val mysql_table_name = "meetup_rsvp_message_agg_detail_tbl"
    val mysql_jdbc_url = "jdbc:mysql://" + myssq_host_name + ":" + mysql_port_no + "/" + mysql_database_name

    val mongo_host_name = "localhost"
    val mongo_port_no = "27017"
    val mongo_user_name = "admin"
    val mongo_passeword = "admin"
    val mongo_database_name = "meetup_rsvp_db"
    val mongo_collection_name = "meetuo_rsvp_message_details_tbl"

    val spark = SparkSession.builder.master(master = "local[*]")
      .appName(name = "Stream Processing Application")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val meetup_df = spark.readStream.format(source = "kafka")
      .option("kafka.bootstrap.servers",kafka_bootstrap_servers)
      .option("subscribe",Kafka_topic_name).load()


    meetup_df.printSchema()

    import spark.implicits._

    val meetup_message_shema = StructType(Array(
      StructField("venue", StructType(Array(
        StructField("venue_name", StringType),
        StructField("lon", StringType),
        StructField("lat", StringType),
        StructField("venue_id", StringType)
      ))),
      StructField("visibility", StringType),
      StructField("response", StringType),
      StructField("guests", StringType),
      StructField("member", StructType(Array(
        StructField("member_id", StringType),
        StructField("photo", StringType),
        StructField("member_name", StringType)
      ))),
      StructField("rsvp_id", StringType),
      StructField("mtime", StringType),
      StructField("event", StructType(Array(
        StructField("event_name", StringType),
        StructField("event_id", StringType),
        StructField("time", StringType),
        StructField("event_url", StringType)
      ))),
      StructField("group", StructType(Array(
        StructField("group_topics", ArrayType(StructType(Array(
          StructField("urlkey", StringType),
          StructField("topic_name", StringType)
        )), true)),
        StructField("group_city", StringType),
        StructField("group_country", StringType),
        StructField("group_id", StringType),
        StructField("group_name", StringType),
        StructField("group_lon", StringType),
        StructField("group_urlname", StringType),
        StructField("group_state", StringType),
        StructField("group_lat", StringType)
      )))
    ))

    val meetup_df1 = meetup_df.selectExpr(exprs =  "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    val meetup_df2 = meetup_df1.select(from_json(functions.col(colName = "value"), meetup_message_shema)
      .functions.as( alias = "message_detail"), functions.col(colName = "timestamp"))

    val meetup_rsvp_df_3 = meetup_df2.select(col = "message_detail", cols = "timestamp")

    val meetup_rsvp_df_4 = meetup_rsvp_df_3.select(col("group.group_name"),
      col("group.group_country"), col("group.group_state"), col("group.group_city"),
      col("group.group_lat"), col("group.group_lon"), col("group.group_id"),
      col("group.group_topics"), col("member.member_name"), col("response"),
      col("guests"), col("venue.venue_name"), col("venue.lon"), col("venue.lat"),
      col("venue.venue_id"), col("visibility"), col("member.member_id"),
      col("member.photo"), col("event.event_name"), col("event.event_id"),
      col("event.time"), col("event.event_url")
    )

    meetup_rsvp_df_4.printSchema()

    val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)

    meetup_rsvp_df_4.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

        batchDF_1.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri)
          .option("database", mongodb_database_name)
          .option("collection", mongodb_collection_name)
          .save()
      }.start()

    val meetup_rsvp_df_5 = meetup_rsvp_df_4.groupBy("group_name", "group_country",
      "group_state", "group_city", "group_lat", "group_lon", "response")
      .agg(count(col("response")).as("response_count"))

    println("Printing Schema of meetup_rsvp_df_5: ")
    meetup_rsvp_df_5.printSchema()

    val trans_detail_write_stream = meetup_rsvp_df_5
      .writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .option("truncate", "false")
      .format("console")
      .start()


    val mysql_properties = new java.util.Properties
    mysql_properties.setProperty("driver", mysql_driver_class)
    mysql_properties.setProperty("user", mysql_user_name)
    mysql_properties.setProperty("password", mysql_password)

    println("mysql_jdbc_url: " + mysql_jdbc_url)

    meetup_rsvp_df_5.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))

        batchDF_1.write.mode("append").jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
      }.start()

    trans_detail_write_stream.awaitTermination()


  }

}
