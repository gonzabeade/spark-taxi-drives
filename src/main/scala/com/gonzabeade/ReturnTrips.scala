package com.gonzabeade

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import java.time.Duration
object ReturnTrips {

  private val EARTH_RADIUS = 6371*1000

  private val makeDistExpr = (lat1: Column, lon1: Column, lat2: Column, lon2: Column) => {
    val dLat = radians(abs(lat2 - lat1))
    val dLon = radians(abs(lon2 - lon1))
    val hav = pow(sin(dLat * 0.5), 2) + pow(sin(dLon * 0.5), 2) * cos(radians(lat1)) * cos(radians(lat2))
    abs(lit(2*EARTH_RADIUS) * asin(sqrt(hav)))
  }

  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._

    val trips1 = trips.alias("trips1")
    val trips2 = trips.alias("trips2")


    trips1
      .crossJoin(trips2)
      .where(makeDistExpr($"trips1.dropoff_latitude", $"trips1.dropoff_longitude", $"trips2.pickup_latitude", $"trips2.pickup_longitude") < dist)
      .where(makeDistExpr($"trips2.dropoff_latitude", $"trips2.dropoff_longitude", $"trips1.pickup_latitude", $"trips1.pickup_longitude") < dist)
      .where($"trips1.tpep_dropoff_datetime" < $"trips2.tpep_pickup_datetime")
      .where($"trips1.tpep_dropoff_datetime" + expr("INTERVAL 8 HOUR") > $"trips2.tpep_pickup_datetime")
  }
}
