package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ReadingUnstructuredData {
  def main(args:Array[String]){
    
    System.setProperty("hadoop.home.dir","C:/hadoop" );
   
    val spark = SparkSession.builder().appName("Reading SemiStructured Data").master("local").getOrCreate()
    
    val mySchema = new StructType()
    .add("TimeStamp", StringType, true)
    .add("Pid", StringType, true)
    .add("Price", StringType, true)
    .add("ViewTime", StringType, true)
    
    val df = spark.read.schema(mySchema).csv("logs.csv")
    //df.show(false)
    
    val df1 = df.withColumn("DateTimeStamp", rtrim(element_at(split(df("TimeStamp"), " "), 1)))
    .withColumn("Channel", rtrim(element_at(split(df("TimeStamp"), " "), 2)))
    .withColumn("Event", rtrim(element_at(split(df("TimeStamp"), " "), 4))).drop("TimeStamp")//.show()
    
    /*
    df1.withColumn("Pid", rtrim(element_at(split(df1("Pid"), "="), 2)))
    .withColumn("Price", rtrim(element_at(split(df1("Price"), "="), 2)))
    .withColumn("ViewTime", rtrim(element_at(split(df1("ViewTime"), "="), 2)))
     .withColumn("Event", rtrim(element_at(split(df1("Event"), "="), 2))).show() */
    
    val cols = List("Pid", "Price", "ViewTime", "Event")
    cols.foldLeft(df1){(new_df, colname) => new_df.withColumn(colname, rtrim(element_at(split(df1(colname), "="), 2)) )}.show()
    
    
       
  }
}
