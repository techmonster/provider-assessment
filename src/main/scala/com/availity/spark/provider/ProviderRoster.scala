package com.availity.spark.provider

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, count, lit, month, concat, year}

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Availity Test")
      .config("spark.master", "local")
      .getOrCreate()
    val providers = spark.read.option("delimiter", "|").option("header","True").csv("data/providers.csv")
    val providers_with_name = providers.withColumn("name", concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
    val visits = spark.read.option("header", "True").csv("data/visits.csv")
    val provider_visits = visits.join(providers_with_name, "provider_id")
    val visit_count_per_provider = provider_visits.groupBy("provider_id", "name", "provider_specialty").count()
    val number_of_visits_per_provider = visit_count_per_provider.withColumnRenamed("count", "number_of_visits")
      .withColumnRenamed("provider_specialty", "specialty")

    val visits_by_month_year = provider_visits.withColumn("year", year(col("date_of_service")))
      .withColumn("month", month(col("date_of_service"))).groupBy("provider_id", "year", "month").count()
    val total_visits_by_month_year = visits_by_month_year.withColumnRenamed("count", "total_visits")
    number_of_visits_per_provider.write.format("json").option("header", "True").partitionBy("specialty").save("data/total_visits_per_provider")
    total_visits_by_month_year.write.format("json").option("header", "True").save("data/total_visits_per_provider_per_month")
  }

}
