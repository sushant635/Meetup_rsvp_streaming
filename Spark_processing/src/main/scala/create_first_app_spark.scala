package com.apachespark
import org.apache.spark.sql.SparkSession
object create_first_app_spark {
  def main(array: Array[String]): Unit = {
    println("started")
    println("First Apache spark using scala")

    val spark = SparkSession
      .builder
      .appName( name ="Apache Spark")
      .master( master = "local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val tech_name_list = List("spark1","spark2","hadoop1","hadoop2","spark3","scala2")
    val names_rdd = spark.sparkContext.parallelize(tech_name_list, numSlices = 3)
    val name_upper_case_rdd = names_rdd.map(ele => ele.toUpperCase())
    name_upper_case_rdd.collect().foreach(println)

    spark.stop()
    println("completed")

  }

}
