# Data Pipeline Automation

Here I will create a data pipeline automation (ETL) system by combining the use of PySpark, Airflow, and MongoDB.

---
## Dataset
From kaggle :
- https://www.kaggle.com/datasets/prasad22/healthcare-dataset
  
## A. Pra Automation
1. Perform data exploration and data validation in notebooks .ipynb
2. Perform a simple data exploration to understand the condition of the dataset, with the goal of identifying the necessary data cleaning and processing steps that will later be developed in the data pipeline.
3. Data validation is performed using Great Expectations. The selected Expectation criteria include:
   - to be unique
   - to be between a minimum and maximum value
   - to be in a defined set
   - to be in a specified type list, and many more.

## B. Data Pipeline Automation
1. Extract = I use psypark to open and read data
2. Transform = Cleaning and processing use psypark with jupyter notebook
3. Load = save data to database MongoDB and make connection to database Mongodb with pymongo 

## C. Workflow Orchestration
Automation by creating DAG, and running it using airflow.  

Special Provisions =
- Scheduling starts on November 1st, 2024.
- Scheduling is conducted every Saturday from 09:10 AM to 09:30 AM, with intervals of 10 minutes.

## 🛠️ Tools
- Python with library 
- Great Expectations (GX) for Data Validation
- Jupyterlab notebook
- Psypark
- Docker
- Airflow
- MongoDB

## 📌 Note
The results of this project are part of my ongoing practice in data analysis.
However, the insights may also be useful for the general public, especially if any practical benefits can be derived from the findings.

