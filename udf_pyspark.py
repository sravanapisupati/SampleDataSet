import statistics as st
from pyspark.sql.types import *

def get_mean(a, b):
  z = st.fmean([a, b])
  return z

mean_udf = udf(get_mean, FloatType())
order_df.withColumn("fmean_data", mean_udf(f.col("order_item_subtotal"), f.col("order_item_product_price")) ).show()
