package com.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column


object DFFunctionsExample {
  
  def main(args: Array[String]) {
    
     System.setProperty("hadoop.home.dir","C:/hadoop" )
    
    val spark = SparkSession.builder().appName("DataFrame APIs").master("local").getOrCreate()
    
    val products = spark.read.csv("products.csv").toDF("product_id","product_category_id","product_name","product_description","product_price","product_image")
  
    val categories = spark.read.csv("categoroes.csv").toDF("category_id","category_department_id","category_name")
    
    val order_items = spark.read.csv("order_items.csv").toDF("order_item_id","order_item_order_id","order_item_product_id","order_item_quantity","order_item_subtotal","order_item_product_price")
    
    //Get the Top 5 Products which are having highest revenue from  the Accessories category
    val resDf = products.select("product_id","product_category_id","product_name")
                .join(categories.select("category_id","category_name"), col("product_category_id") === col("category_id"))
                .join( order_items.select("order_item_id","order_item_product_id","order_item_subtotal"), col("product_id") === col("order_item_product_id"))
                .filter(col("category_name") === "Accessories")
                .groupBy("category_name", "product_name")
                .agg(round(sum(col("order_item_subtotal")),2).as("revenue"))
                .withColumn("rank", rank() over Window.partitionBy("category_name").orderBy("revenue"))
                .orderBy(desc("revenue"))
                .drop("rank")
                .limit(5)
               // .show()
                
     products.createOrReplaceTempView("products")
     categories.createOrReplaceTempView("categories")
     order_items.createOrReplaceTempView("order_items")
     
     val resultDf = spark.sql("""select category_name, product_name, product_revenue from (select category_name, product_name, Round(sum(order_item_subtotal),2) product_revenue, 
                                 rank() over (partition by category_name order by Round(sum(order_item_subtotal),2) desc) rank from products p 
                                 join categories c on p.product_category_id = c.category_id 
                                 join order_items o  on p.product_id = o.order_item_product_id where category_name = 'Accessories' group by category_name, product_name ) t1 where t1.rank <= 5""")
                                 
     // resultDf.show()     
      
      resultDf.explain(mode="extended")
  }
  
}
