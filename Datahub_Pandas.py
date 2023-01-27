import pandas as pd
import numpy as np

df_location = pd.read_csv ('location.csv')
print('Location')
print(df_location)

df_product = pd.read_csv ('product.csv')
print('Product')
print(df_product)

df_1 = pd.read_csv ('trans_fact_1.csv')
print('df_1')
print(df_1)

df_2 = pd.read_csv ('trans_fact_2.csv')
print('df_2')
print(df_2)

df_3 = pd.read_csv ('trans_fact_3.csv')
print('df_3')
print(df_3)

df_4 = pd.read_csv ('trans_fact_4.csv')
print('df_4')
print(df_4)

df_5 = pd.read_csv ('trans_fact_5.csv')
print('df_5')
print(df_5)

df_6 = pd.read_csv ('trans_fact_6.csv')
print('df_6')
print(df_6)

df_7 = pd.read_csv ('trans_fact_7.csv')
print('df_7')
print(df_7)

df_8 = pd.read_csv ('trans_fact_8.csv')
print('df_8')
print(df_8)

df_9 = pd.read_csv ('trans_fact_9.csv')
print('df_9')
print(df_9)

df_10 = pd.read_csv ('trans_fact_10.csv')
print('df_10')
print(df_10)

union_dfs = pd.concat([df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10])
print('union_dfs')
print(union_dfs)

df_union_product = union_dfs.merge(df_product, on='product_key', how='left')
print('df_union_product')
print(df_union_product)

df_union_product_location = df_union_product.merge(df_location, on='store_location_key', how='left')
print('df_union_product_location')
print(df_union_product_location)
print(df_union_product_location.info())

df = df_union_product_location.fillna(0)
#pd.set_option('display.max_columns', None)
#pd.set_option("display.max_rows", None)

print('df')
print(df)

avg_sales_by_province = df.groupby('province').sales.agg('mean')
avg_sales_by_province = avg_sales_by_province.to_frame()
avg_sales_by_province.reset_index(inplace=True)
avg_sales_by_province = avg_sales_by_province.set_index('province')
print('avg_sales_by_province')
print(avg_sales_by_province)
print(avg_sales_by_province.info())

avg_sales_by_province_by_store = df.groupby(['province', 'store_num']).sales.agg('mean')
avg_sales_by_province_by_store = avg_sales_by_province_by_store.to_frame()
avg_sales_by_province_by_store.reset_index(inplace=True)
avg_sales_by_province_by_store = avg_sales_by_province_by_store.set_index('province')
avg_sales_by_province_by_store['avg_sales_province'] = avg_sales_by_province['sales']
print('avg_sales_by_province_by_store')
print(avg_sales_by_province_by_store)
print(avg_sales_by_province_by_store.info())


result_1_df = avg_sales_by_province_by_store[avg_sales_by_province_by_store['sales'] > avg_sales_by_province_by_store['avg_sales_province']] 
print('result_1_df')
print(result_1_df)

result_1_df.to_csv('First.csv')

######################################Second question
df['Loyal_Customer'] = np.where(df['collector_key'] >= 0, 'Loyal','Non Loyal')
print('df')
print(df)
print(df.info())

sum_sales_by_category_by_loyal = df.groupby(['category', 'Loyal_Customer']).sales.agg('sum')
sum_sales_by_category_by_loyal = sum_sales_by_category_by_loyal.to_frame()
sum_sales_by_category_by_loyal.reset_index(inplace=True)
print('sum_sales_by_category_by_loyal')
print(sum_sales_by_category_by_loyal)
print(sum_sales_by_category_by_loyal.info())
sum_sales_by_category_by_loyal.to_csv('Second.csv')

######################################Second question

##Part A Top 5 Stores by province

sum_sales_by_province_by_store = df.groupby(['province', 'store_num']).sales.agg('sum')
sum_sales_by_province_by_store = sum_sales_by_province_by_store.to_frame()
sum_sales_by_province_by_store.reset_index(inplace=True)
print('sum_sales_by_province_by_store')
print(sum_sales_by_province_by_store)

sum_sales_by_province_by_store['rank'] = sum_sales_by_province_by_store.groupby(['province'])['sales'].rank('dense', ascending=False)
print('sum_sales_by_province_by_store')
print(sum_sales_by_province_by_store)

sum_sales_by_province_by_store = sum_sales_by_province_by_store.sort_values(['province', 'rank'])
print('sum_sales_by_province_by_store')
print(sum_sales_by_province_by_store)

sum_sales_by_province_by_store = sum_sales_by_province_by_store[sum_sales_by_province_by_store['rank']<=5]
print('sum_sales_by_province_by_store')
print(sum_sales_by_province_by_store)
sum_sales_by_province_by_store.to_csv('Third_A.csv')

##Part B Top 10 Product Sales Categories by Department
sum_sales_by_category_by_department = df.groupby(['department', 'category']).sales.agg('sum')
sum_sales_by_category_by_department = sum_sales_by_category_by_department.to_frame()
sum_sales_by_category_by_department.reset_index(inplace=True)
print('sum_sales_by_category_by_department')
print(sum_sales_by_category_by_department)

sum_sales_by_category_by_department['rank'] = sum_sales_by_category_by_department.groupby(['department'])['sales'].rank('dense', ascending=False)
print('sum_sales_by_category_by_department')
print(sum_sales_by_category_by_department)

sum_sales_by_category_by_department = sum_sales_by_category_by_department.sort_values(['department', 'rank'])
print('sum_sales_by_category_by_department')
print(sum_sales_by_category_by_department)

sum_sales_by_category_by_department = sum_sales_by_category_by_department[sum_sales_by_category_by_department['rank']<=10]
print('sum_sales_by_category_by_department')
print(sum_sales_by_category_by_department)
sum_sales_by_category_by_department.to_csv('Third_B.csv')
