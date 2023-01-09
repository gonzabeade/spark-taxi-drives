package com.gonzabeade

import org.apache.spark.sql.SparkSession



/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val spark = SparkSession
      .builder
      .appName("ReturnTripFinder Test")
      .config("spark.master", "local")
      .getOrCreate()
    import spark.implicits._



    println( "Hello World!" )
    println("concat arguments = " + foo(args))
  }

}
