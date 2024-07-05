import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, input_file_name, lit
from datetime import datetime, timedelta


def process(hdfs_directory, database_path):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Process News") \
        .master("local[4]") \
        .getOrCreate()

    #get the latest file from news directory 
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")# List all files in the directory
    files_df = files_df.withColumn("file_name", input_file_name())# Extract file names and modification times
    latest_parquet_file_path = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()[0].file_name# Order files by modification time descending and get the latest file

    # Connect to DuckDB
    conn = duckdb.connect(database=database_path)
    
    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(latest_parquet_file_path)
    df_spark.show()
    # Step 1: Create DataFrame for dim_topics and insert new topics if they do not exist
    df_topics = df_spark.select(explode(col("topics")).alias("topic")) \
                        .select("topic.topic").distinct().withColumnRenamed("topic", "topic_name")

    # Convert Spark DataFrame to Arrow Table
    arrow_table_topics = df_topics.toPandas()

    # Insert data into dim_topics
    conn.register('arrow_table_topics', arrow_table_topics)
    conn.execute("""
        INSERT INTO dim_topics (topic_name)
        SELECT * FROM arrow_table_topics
        WHERE topic_name NOT IN (SELECT topic_name FROM dim_topics)
    """)
    print("Data inserted into dim_topics successfully!")
    
    # Step 2: Get yesterday's date and insert new time data into dim_time if it does not exist
    yesterday = datetime.now().date() - timedelta(days=1)
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')
    
    # Get corresponding time_id from dim_time
    id_time_df = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchdf()
        
    news_time_id = id_time_df['time_id'][0]

    # Step 3: Create DataFrame for dim_news
    df_news = df_spark.select(
        col("title").alias("new_title"),
        col("url").alias("new_url"),
        col("time_published").alias("new_time_published"),
        col("authors").alias("new_authors"),
        col("summary").alias("new_summary"),
        col("source").alias("new_source"),
        col("overall_sentiment_score").alias("new_overall_sentiment_score"),
        col("overall_sentiment_label").alias("new_overall_sentiment_label")
    ).withColumn("news_time_id", lit(news_time_id))
    
    # Convert Spark DataFrame to Arrow Table
    arrow_table_news = df_news.toPandas()
    
    print(arrow_table_news)

    # Step 4: Insert DataFrame dim_news into dim_news table
    conn.register('arrow_table_news', arrow_table_news)
    conn.execute('''
        INSERT INTO dim_news (
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        ) SELECT 
            new_title,
            new_url,
            new_time_published,
            new_authors,
            new_summary,
            new_source,
            new_overall_sentiment_score,
            new_overall_sentiment_label,
            news_time_id
        FROM arrow_table_news
    ''')
    print("Data inserted into dim_news successfully!")
    
    # Step 5: Create DataFrame for fact_news_topics
    df_fact_news_topics = df_spark.select(
        explode(col("topics")).alias("topic"),
        col("title").alias("new_title")
    ).select(
        col("topic.relevance_score").alias("new_topic_relevance_score"),
        col("topic.topic").alias("topic_name"),
        col("new_title")
    )
    arrow_table_fact_news_topics = df_fact_news_topics.toPandas()
    
    # Get corresponding topic_id from dim_topics
    id_topic_df = conn.execute('SELECT * FROM dim_topics').fetchdf()
    arrow_table_fact_news_topics = arrow_table_fact_news_topics.merge(id_topic_df, on='topic_name', how='left')
    arrow_table_fact_news_topics = arrow_table_fact_news_topics[arrow_table_fact_news_topics['topic_id'].notnull()]
    
    # Get corresponding new_id from dim_news
    id_new_df = conn.execute('SELECT new_id, new_title FROM dim_news').fetchdf()
    arrow_table_fact_news_topics = arrow_table_fact_news_topics.merge(id_new_df, on='new_title', how='left')
    arrow_table_fact_news_topics = arrow_table_fact_news_topics[arrow_table_fact_news_topics['new_id'].notnull()]

    # Load DataFrame into fact_news_topics table
    conn.register('arrow_table_fact_news_topics', arrow_table_fact_news_topics)
    conn.execute('''
        INSERT INTO fact_news_topics (
            new_topic_new_id,
            new_topic_topic_id,
            new_topic_relevance_score
        ) SELECT 
            new_id,
            topic_id,
            new_topic_relevance_score
        FROM arrow_table_fact_news_topics
    ''')
    print("Data inserted into fact_news_topics successfully!")
    
    # Step 6: Create DataFrame for fact_news_companies
    df_fact_news_companies = df_spark.select(
        explode(col("ticker_sentiment")).alias("ticker_sentiment"),
        col("title").alias("new_title")
    ).select(
        col("ticker_sentiment.relevance_score").alias("new_company_relevance_score"),
        col("ticker_sentiment.ticker").alias("company_ticket"),
        col("ticker_sentiment.ticker_sentiment_score").alias("new_company_ticker_sentiment_score"),
        col("ticker_sentiment.ticker_sentiment_label").alias("new_company_ticker_sentiment_label"),
        col("new_title")
    )
    arrow_table_fact_news_companies = df_fact_news_companies.toPandas()
    
    # Get corresponding new_id from dim_news
    arrow_table_fact_news_companies = arrow_table_fact_news_companies.merge(id_new_df, on='new_title', how='left')
    arrow_table_fact_news_companies = arrow_table_fact_news_companies[arrow_table_fact_news_companies['new_id'].notnull()]
    
    # Get corresponding company_id from dim_companies
    query = """
        SELECT company_id, company_ticket, company_time_stamp
        FROM (
            SELECT 
                company_id, 
                company_ticket, 
                company_time_stamp,
                ROW_NUMBER() OVER (PARTITION BY company_ticket ORDER BY company_time_stamp DESC) as row_num
            FROM dim_companies
        ) subquery
        WHERE row_num = 1;
    """
    id_company_df = conn.execute(query).fetchdf()
    arrow_table_fact_news_companies = arrow_table_fact_news_companies.merge(id_company_df, on='company_ticket', how='left')
    arrow_table_fact_news_companies = arrow_table_fact_news_companies[arrow_table_fact_news_companies['company_id'].notnull()]
    
    print(arrow_table_fact_news_companies)

    # Load DataFrame into fact_news_companies table
    conn.register('arrow_table_fact_news_companies', arrow_table_fact_news_companies)
    conn.execute('''
        INSERT INTO fact_news_companies (
            new_company_company_id,
            new_company_new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        ) SELECT 
            company_id,
            new_id,
            new_company_relevance_score,
            new_company_ticker_sentiment_score,
            new_company_ticker_sentiment_label
        FROM arrow_table_fact_news_companies
    ''')
    print("Data inserted into fact_news_companies successfully!")
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

def process_news_to_datawarehouse():
    hdfs_directory = "/stock-market-data/news/raw" # hdfs path
    database_path = "/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb" # database path

    process(hdfs_directory, database_path)
    
if __name__ == "__main__":
    process_news_to_datawarehouse()