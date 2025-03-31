package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataSkewExample {

  def main(args:Array[String]){
    
    System.setProperty("hadoop.home.dir","C:/hadoop" );
    
    val spark = SparkSession.builder().appName("Dataskew Problem in Spark").config("spark.ui.port","4050").master("local").getOrCreate()
    
    import spark.implicits._
   
    val normalUsers = (0 until 1000).map( i => (s"user_${i % 100}", i, s"2025-03-${i % 30 + 1}"))
    val skewedUsers = (0 until 200000).map( i => (s"user_999", i, s"2025-03-${i % 30 + 1}"))
    
    val data = normalUsers ++ skewedUsers
    
    val dfSkewed = data.toDF("user_id", "transaction_id", "transaction_date")
    
    val dfSkewedPartitioned = dfSkewed.repartition(10, col("user_id"))
    
    dfSkewedPartitioned.write.mode("overwrite").parquet("skewed_dataset")
    val smallData = (0 until 100).map( i => (s"user_${i}", s"category_${i % 10}")).toDF("user_id", "category")
    smallData.write.mode("overwrite").parquet("small_dataset")
          
    val dfSkewedData = spark.read.parquet("skewed_dataset")
    val dfSmall = spark.read.parquet("small_dataset")
    
    val dfJoined = dfSkewedData.join(dfSmall, "user_id")
    
    dfJoined.write.mode("overwrite").parquet("joined_dataset")
    
    // Lets apply the salting technique
    
    val dfSalted = dfSkewedData.withColumn("salted_user_id", concat(col("user_id"), lit("_"), (rand() * 10).cast("int")))
    
    val dfSmallSalted = dfSmall.withColumn("salted_user_id", concat(col("user_id"), lit("_"), lit(0)))
    
    val dfResultsJoinedSalted = dfSalted.join(dfSmallSalted, "salted_user_id")
    
    dfResultsJoinedSalted.write.mode("overwrite").parquet("final_output")
    
    
    
    
  }
  
}
