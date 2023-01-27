from pyspark.sql.functions import when
from pyspark.sql.functions import col

print(spark)

#df=spark.sql("show databases")
#df.show()

#df=spark.sql("SHOW TABLES FROM default")
#df.show()

####1-Ingestion, files had unsorted colums and same trans colum with different names 

df_location = spark.table('location')
df_product = spark.table('product')
df_1 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_key FROM trans_fact_1')
df_2 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_key FROM trans_fact_2')
df_3 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_key FROM trans_fact_3')
df_4 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_key FROM trans_fact_4')
df_5 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_key FROM trans_fact_5')
df_6 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_id as trans_key FROM trans_fact_6')
df_7 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_id as trans_key FROM trans_fact_7')
df_8 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_id as trans_key FROM trans_fact_8')
df_9 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_id as trans_key FROM trans_fact_9')
df_10 = spark.sql('SELECT store_location_key, product_key, collector_key, trans_dt, sales, units, trans_id as trans_key FROM trans_fact_10')

####2-Union, Joining and cleaning

df_union = df_1.union(df_2).union(df_3).union(df_4).union(df_5).union(df_6).union(df_7).union(df_8).union(df_9).union(df_10)

df_union.write.mode("overwrite").saveAsTable("union_trans")

df_union_product_location = spark.sql("""
SELECT u.product_key, p.department, p.category, u.store_location_key, l.province, u.sales, u.collector_key FROM union_trans u
LEFT JOIN product p on u.product_key = p.product_key 
LEFT JOIN location l on u.store_location_key = l.store_location_key
;
""")
df_union_product_location.na.fill(0)

df_union_product_location.write.mode("overwrite").saveAsTable("union_product_location")

####3 Insights 
####3.1 - Avg sale by province by store above Avg province
df_avg_sales_by_province = spark.sql("""
SELECT province, avg(sales) as sales_province
FROM union_product_location
WHERE province is not null
GROUP BY province
;
""")
df_avg_sales_by_province.write.mode("overwrite").saveAsTable("avg_sales_by_province")

df_avg_sales_by_province_by_store = spark.sql("""
SELECT province, store_location_key,  avg(sales) as sales_province_store
FROM union_product_location
WHERE province is not null
GROUP BY province, store_location_key
;
""")
df_avg_sales_by_province_by_store.write.mode("overwrite").saveAsTable("avg_sales_by_province_by_store")
df_avg_sales_by_province_by_store.show()

df_avg_sales_by_province_by_store_above_avg_province = spark.sql("""
SELECT * FROM (
  SELECT ps.province, ps.store_location_key, ps.sales_province_store, p.sales_province
  FROM avg_sales_by_province_by_store AS ps
  LEFT JOIN avg_sales_by_province AS p 
  ON ps.province = p.province
WHERE sales_province_store >= sales_province
)
;
""")
df_avg_sales_by_province_by_store_above_avg_province.show()
df_avg_sales_by_province_by_store_above_avg_province.write.mode("overwrite").saveAsTable("avg_sales_by_province_by_store_above_avg_province")

####3.2 - Sum sales by loyalty and sum sales by category by loyalty
df_union_product_location = df_union_product_location

df_union_product_location_loyalty = df_union_product_location.withColumn("loyalty", when(col("collector_key") >= 0,"Loyal").otherwise("Nonloyal"))
df_union_product_location_loyalty.show()
df_union_product_location_loyalty.write.mode("overwrite").saveAsTable("union_product_location_loyalty")

df_sum_sales_by_loyalty = spark.sql("""
  SELECT loyalty, sum(sales) as sales_by_loyalty
  FROM union_product_location_loyalty
  GROUP BY loyalty
;
""")
df_sum_sales_by_loyalty.show()
df_sum_sales_by_loyalty.write.mode("overwrite").saveAsTable("sum_sales_by_loyalty")

df_sum_sales_by_loyalty_by_category = spark.sql("""
  SELECT loyalty, category, sum(sales) as sales_by_loyalty_by_category
  FROM union_product_location_loyalty
  GROUP BY loyalty, category
;
""")
df_sum_sales_by_loyalty_by_category.show()
df_sum_sales_by_loyalty_by_category.write.mode("overwrite").saveAsTable("sum_sales_by_loyalty_by_category")

####3.3.1 - Top 5 Sales by Province by Store
df_top_5_sales_by_province_by_store = spark.sql("""
  SELECT * FROM (
    SELECT province, store_location_key, sum(sales) as province_store_sales, RANK() OVER(PARTITION BY province ORDER BY sum(sales) DESC) as sales_province_rank
    FROM union_product_location
    GROUP BY province, store_location_key)
  WHERE sales_province_rank <= 5  
;
""")
df_top_5_sales_by_province_by_store.show()
df_top_5_sales_by_province_by_store.write.mode("overwrite").saveAsTable("top_5_sales_by_province")

####3.3.2 - Top 10 Sales by Departments by Categories
df_top_10_sales_by_department_by_categories = spark.sql("""
  SELECT * FROM (
    SELECT department, category, sum(sales) as department_category_sales, RANK() OVER(PARTITION BY department ORDER BY sum(sales) DESC) as sales_department_rank
    FROM union_product_location
    GROUP BY department, category)
  WHERE sales_department_rank <= 10  
;
""")
df_top_10_sales_by_department_by_categories.show()
df_top_10_sales_by_department_by_categories.write.mode("overwrite").saveAsTable("top_10_sales_by_department_by_categories")
