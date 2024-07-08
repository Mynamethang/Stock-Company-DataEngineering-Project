<h1>Introducing My US Stock Market Project 📊📈📈 </h1>

<h1>Overview</h1>

# 1 Process

- **_In this project I deploy an ETL process, initialize suitable storage and BI system for data of stock._** 
- **_The data in this project is extracted from data source of the US stock market._**
- **_I use various of tools including big data tools to perform complex tasks_**
- **_Integrate the automation workflow in this project to trigger tasks in right time_**
- **_Create dashboard model for analytics purpose_**


# 2 Processing Pipeline
## _Tools that I used to deploy the data pipeline and storage_

<img src="img\Project Architect.png" alt="Processing Pipeline">  


# 3 Data warehouse  🏭

 <img src="img\Galaxy Schema.png" alt="Schema" width="600" height="500">

# 4 Dashboards 📊

<div style="display: flex; flex-wrap: wrap; gap: 20px;">
    <img src="dashboards/dashboard1.png" alt="Dashboard 1" width="300" height="200">
    <img src="dashboards/dashboard2.png" alt="Dashboard 2" width="300" height="200">
    <img src="dashboards/dashboard3.png" alt="Dashboard 3" width="300" height="200">
</div>

<div style="display: flex; flex-wrap: wrap; gap: 20px;">
    <img src="dashboards/dashboard4.png" alt="Dashboard 4" width="300" height="200">
    <img src="dashboards/dashboard5.png" alt="Dashboard 5" width="300" height="200">
    <img src="dashboards/dashboard6.png" alt="Dashboard 6" width="300" height="200">
</div>

<div style="display: flex; flex-wrap: wrap; gap: 20px;">
    <img src="dashboards/dashboard7.png" alt="Dashboard 7" width="300" height="200">
</div>


 _link doashboards documents_
https://drive.google.com/file/d/1jW4DsNfhnjfpGlTw9MevLFmh5RhHWkMV/view?usp=drive_link

<h1>Implement</h1>

1. **Run Aiflow** :

 ```Bash
 airflow scheduler
 airflow webserver --port 8080
 ```
![Screenshot 2024-07-05 143945](https://github.com/Mynamethang/Stock-Company-DataEngineering-Project/assets/109019819/97c162df-6a6f-42ec-8d0d-2a0425f67637)


2. **Query & Check If data is inserted to Data Warehouse correctly** :
 ```Bash
 /home/ngocthang/Documents/code/Airflow-ven/airflow_venv/bin/python /home/ngocthang/Documents/code/Stoct-Project/SQL/config-datawarehouse/config.py
 ```
![image](https://github.com/Mynamethang/Stock-Company-DataEngineering-Project/assets/109019819/a562615d-511a-417c-9274-2d4fa2738d97)

3. **Run API Application and Open the API in Local Machine** :
 ```Bash
 python /home/ngocthang/Documents/code/Stoct-Project/api-datawarehouse.py
 ```
![Screenshot 2024-07-05 153410](https://github.com/Mynamethang/Stock-Company-DataEngineering-Project/assets/109019819/e8f1171d-4814-40a2-9e3d-f31ab04eadd1)

4. **Extract data from the data warehouse API** :
   
   a. Access directory: <span style="color: red;">data-for-dashboards/request</span>
   b Run the file : <span style="color: red;">request-data.py</span>




