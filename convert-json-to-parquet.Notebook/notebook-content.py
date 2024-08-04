# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3565efb6-0f46-4fa1-9dad-a8188d4c0550",
# META       "default_lakehouse_name": "DemoLH",
# META       "default_lakehouse_workspace_id": "09fade75-1d20-4aea-ac5b-305cdb6aee23",
# META       "known_lakehouses": [
# META         {
# META           "id": "3565efb6-0f46-4fa1-9dad-a8188d4c0550"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# import json
# import os
# import pandas as pd
# 
# # Set the path to the directory containing the raw JSON data
# raw_data_path = '/lakehouse/default/Files/raw-data'
# 
# # Get the list of JSON files in the raw data path, and select the first 10 for the training set
# train_set = os.listdir(raw_data_path)[:10]
# 
# # Select the 11th file for the test set
# test_set = os.listdir(raw_data_path)[10]
# 
# # Initialize empty DataFrames to store images, annotations, and categories data
# images = pd.DataFrame()
# annotations = pd.DataFrame()
# categories = pd.DataFrame()
# 
# # Process each JSON file in the training set
# for file in train_set:
#     # Read the JSON file and load its data
#     path = os.path.join(raw_data_path, file)
#     with open(path) as f:
#         data = json.load(f)
# 
#     # Extract and concatenate the 'images' and 'annotations' data into their respective DataFrames
#     images = pd.concat([images, pd.DataFrame(data['images'])])
#     annotations = pd.concat([annotations, pd.DataFrame(data['annotations'])])
# 
#     # The 'categories' data is the same for all files, so we only need to do it once
#     if len(categories) == 0:
#         categories = pd.DataFrame(data['categories'])
# 
# # Set the path to the directory where the processed data will be saved
# data_path = '/lakehouse/default/Files/data'
# 
# # Create the directory if it doesn't exist
# if not os.path.exists(data_path):
#     os.makedirs(data_path)
# 
# # Define the file paths for saving the training data as Parquet files
# train_images_file = os.path.join(data_path, 'train_images.parquet')
# train_annotations_file = os.path.join(data_path, 'train_annotations.parquet')
# categories_file = os.path.join(data_path, 'categories.parquet')
# 
# # Convert the DataFrames to Parquet format using the pyarrow engine with snappy compression
# images.to_parquet(train_images_file, engine='pyarrow', compression='snappy')
# annotations.to_parquet(train_annotations_file, engine='pyarrow', compression='snappy')
# categories.to_parquet(categories_file, engine='pyarrow', compression='snappy')
# 
# # Process the test set, similar to the training set
# path = os.path.join(raw_data_path, test_set)
# with open(path) as f:
#     data = json.load(f)
# 
# # Define the file paths for saving the test data as Parquet files
# test_images_file = os.path.join(data_path, 'test_images.parquet')
# test_annotations_file = os.path.join(data_path, 'test_annotations.parquet')
# 
# # Extract and convert the 'images' and 'annotations' data of the test set into DataFrames,
# # then save them as Parquet files with pyarrow engine and snappy compression
# test_images = pd.DataFrame(data['images'])
# test_annotations = pd.DataFrame(data['annotations'])
# 
# test_images.to_parquet(test_images_file, engine='pyarrow', compression='snappy')
# test_annotations.to_parquet(test_annotations_file, engine='pyarrow', compression='snappy')


# CELL ********************



import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/new file/longlookback.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/new file/longlookback.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/new file/query_result.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/new file/query_result.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM DemoLH.categories LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/new file/longlookback.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/new file/longlookback.csv")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC df = spark.sql("SELECT * FROM DemoLH.train_images LIMIT 1000")
# MAGIC display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC df = spark.sql("SELECT * FROM DemoLH.openailogs LIMIT 1000")
# MAGIC display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
