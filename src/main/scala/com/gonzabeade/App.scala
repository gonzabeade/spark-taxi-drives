package com.gonzabeade

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {

    val spark = SparkSession
      .builder
      .appName("ReturnTripFinder Test")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    val taxifile = args(0)
    val schema = StructType(Array(
      StructField("VendorID", DataTypes.StringType, false),
      StructField("tpep_pickup_datetime", DataTypes.TimestampType, false),
      StructField("tpep_dropoff_datetime", DataTypes.TimestampType, false),
      StructField("passenger_count", DataTypes.IntegerType, false),
      StructField("trip_distance", DataTypes.DoubleType, false),
      StructField("pickup_longitude", DataTypes.DoubleType, false),
      StructField("pickup_latitude", DataTypes.DoubleType, false),
      StructField("RatecodeID", DataTypes.IntegerType, false),
      StructField("store_and_fwd_flag", DataTypes.StringType, false),
      StructField("dropoff_longitude", DataTypes.DoubleType, false),
      StructField("dropoff_latitude", DataTypes.DoubleType, false),
      StructField("payment_type", DataTypes.IntegerType, false),
      StructField("fare_amount", DataTypes.DoubleType, false),
      StructField("extra", DataTypes.DoubleType, false),
      StructField("mta_tax", DataTypes.DoubleType, false),
      StructField("tip_amount", DataTypes.DoubleType, false),
      StructField("tolls_amount", DataTypes.DoubleType, false),
      StructField("improvement_surcharge", DataTypes.DoubleType, false),
      StructField("total_amount", DataTypes.DoubleType, false)
    ))

    val tripsDF = spark.read.schema(schema).option("header", true).csv(taxifile)
    val trips = tripsDF
      .where($"pickup_longitude" =!= 0 && $"pickup_latitude" =!= 0 && $"dropoff_longitude" =!= 0 && $"dropoff_latitude" =!= 0)
      .cache()

    val dist = 100 // Get distance in metres from the driver
    val result = ReturnTrips
      .compute(trips, dist, spark)
      .agg(count("*"))
      .first
      .getLong(0)

    println("THE COUNT RESULT IS %d".format(result))
    spark.stop()
  }

}
