package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, concat, count, lit, month, year}

object ProviderRoster  {

  def get_provider_visits(spark: SparkSession): DataFrame = {
    val providers = spark.read.option("delimiter", "|").option("header", "True").csv("data/providers.csv")
    val visits = spark.read.option("header", "True").csv("data/visits.csv")

    val providers_with_name = providers.withColumn("name",
      concat(col("first_name"), lit(" "), col("middle_name"), lit(" "), col("last_name")))
    visits.join(providers_with_name, "provider_id")
  }

  def get_number_of_visits_per_provider(df: DataFrame): DataFrame = {
    val visit_count_per_provider = df.groupBy("provider_id", "name", "provider_specialty").count()

    visit_count_per_provider.withColumnRenamed("count", "number_of_visits")
      .withColumnRenamed("provider_specialty", "specialty")
  }

  def get_total_visits_per_mont_year(df: DataFrame): DataFrame = {
    val visits_by_month_year = df.withColumn("year", year(col("date_of_service")))
      .withColumn("month", month(col("date_of_service")))
      .groupBy("provider_id", "year", "month").count()

    visits_by_month_year.withColumnRenamed("count", "total_visits")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Availity Test")
      .config("spark.master", "local")
      .getOrCreate()
    
    val provider_visits = get_provider_visits(spark)

    val number_of_visits_per_provider = get_number_of_visits_per_provider(provider_visits)
    number_of_visits_per_provider.write.format("json").option("header", "True")
      .partitionBy("specialty").save("data/total_visits_per_provider")

    val total_visits_by_month_year = get_total_visits_per_mont_year(provider_visits)
    total_visits_by_month_year.write.format("json").option("header", "True")
      .save("data/total_visits_per_provider_per_month")
  }

}
