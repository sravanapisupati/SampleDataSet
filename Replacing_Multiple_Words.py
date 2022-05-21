Create a file comments.csv using the below content:
emp_id,emp_name,Comments
111,Apple,He is from IND. He has a HS. He loves MR. His native is KTK.
112,Banana,He is from US. He has a VL. He loves CF. His native is IND.
113,Carrot,He is from UK. He has a FM. He loves LND. His native is KDLG.
114,Cabbage,He is from IND. He has a FT. He loves TN. His native is CBR.
115,Beans,He is from CHN. He has a BGL. He loves HK. His native is WHN.
116,Grapes,He is from IND. He has a HS. He loves JK. His native is AP.

Upload the above file in databricks notebook: https://youtu.be/RmM8jb6k7kI  
Code:
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/comments.csv")
df1.show(truncate=False)

WordsDict={'IND': 'India','CF':'California','MR':'MahaRashtra','HS':'House','US':'United States of America','KTK': 'Karnataka','AP':'AndhraPradesh','WHN': 'Wouhan','CHN':'China','VL':'Villa','BGL':'Bunglow','TN':'Tamilnadu','JK':'Jammu&Kashmir','FT':'Flat','FM':'Form','UK':'United Kingdom','HK':'Hongkong','LND':'London','CBR':'Coimbatore', 'KDLG':'Kidlington'}

def replace_words(comments, WordsDict):
  for key, value in WordsDict.items():
    comments = comments.replace(key, value)
  return comments

replace_udf = f.udf(lambda comments:replace_words(comments, WordsDict) )

res_df = df1.withColumn("New_Comments", replace_udf(f.col("Comments")))
res_df.show(truncate=False) 
