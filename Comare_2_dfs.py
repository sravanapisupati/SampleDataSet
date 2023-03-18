df1 = spark.read.format("csv").option("header", "true").load("location_details.csv")
df2 = spark.read.format("csv").option("header", "true").load("location_details_1.csv")

def find_mismatch(df1, df2, key_list):

  def is_mismatch(col):
    values = (df1[col] != df2[col]) 
           # if( abs(df1[col] - df2[col] > EPSILON))
    return values | (df1[col].isNull() & df2[col].isNotNull()) | (df1[col].isNotNull() & df2[col].isNull())
  
  result = df1.join(df2, key_list, "outer")
  result = result.select(*[result[i] for i in key_list], *[is_mismatch(i).alias(i + '_mismatch')
                                                          for i in df1.columns if i not in key_list])
  mismatched_cols = result.where(reduce(lambda acc, e:acc | e, [result[c+ '_mismatch'] for c in df1.columns if c not in key_list], lit(False)))
  mismatch_count = mismatched_cols.count()
  return mismatch_count, mismatched_cols

mismatch_count, mismatches = find_mismatch(df1, df2,["id", "col1", "col2"])
print(mismatch_count)
mismatches.show()
