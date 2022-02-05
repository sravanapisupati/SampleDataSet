import pyspark.sql.functions as f
df1 = spark.read.format("csv").option("header", "true").option("multiline", "true").load("dbfs:/FileStore/shared_uploads/sravana.pisupati@gmail.com/user_data-3.csv")

df1.show()

df2 = df1.select("id",
                 f.when(f.col("ind") == f.lit("FN"), f.col("fname")).otherwise("null").alias("fname"), 
                 f.when(f.col("ind") == f.lit("LN"), f.col("fname")).otherwise("null").alias("lname"), 
                 f.when(f.col("ind") == f.lit("AD"), f.concat_ws(",", f.col("fname"), f.col("lname"), f.col("apartment"), f.col("street"))).otherwise("null").alias("address"), f.when(f.col("ind") == f.lit("PH"), f.col("fname")).otherwise("null").alias("phone") )
df2.show(truncate=False)

df3 = df2.groupby("id").agg(f.min("fname").alias("fname"), f.min("lname").alias("lname"), f.min("address").alias("address"), f.min("phone").alias("phone"))
df3.show(truncate=False)

df4 = df3.filter((f.col("fname") != f.lit("null")) & (f.col("lname") != f.lit("null")))
df4.show()

df5 = df4.withColumn("apartment", f.split(f.col("address"), ',').getItem(0)) \
       .withColumn('street', f.split(f.col("address"), ',').getItem(1)) \
       .withColumn('city', f.split(f.col("address"), ',').getItem(2))\
       .withColumn('country',f.split(f.col("address"), ',').getItem(3))
df5.select("id", "fname", "lname", "apartment", "street", "city", "country", "phone").show(truncate=False)

//Sample Input file:
  
  id,ind,fname,lname,apartment,street,city,country,phone
001,FN,John
001,LN,Wick
001,AD,22,Continental Hotel, New York, USA
001,PH,9999999999
002,FN,Sherlock
002,LN,Homes
002,AD,44,Baker St, London, UK
002,PH,8888888888
003,FN,Ted
003,LN,Lasso
003,AD,AFC,Richmond, London, UK
003,PH,3336669990
004,FN,Star
004,LN,Lord
004,AD,
004,PH,
005,FN,Clark
005,LN,
005,AD,44,Daily Planet, Metropolis, USA
005,PH,1234567890
006,FN,Bruce
006,LN,Wayne
006,AD,1007,Mountain Drive, Gotham, USA
006,PH,1111111111
007,FN,Sachin
007,LN,Chavan
007,AD,2013,Mallesh Palya, Karnataka, India
007,PH,1111111111
008,FN,
008,LN,Chavan
008,AD,2013,Mallesh Palya, Karnataka, India
008,PH,
009,FN,Dundappa
