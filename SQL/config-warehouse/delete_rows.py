import duckdb

database_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb'
con = duckdb.connect(database=database_path)  # Use ':memory:' for in-memory database


# Deleting rows from child tables first
con.execute("DELETE FROM fact_news_companies")
con.execute("DELETE FROM fact_news_topics")
con.execute("DELETE FROM fact_candles")

#Then delete rows from parent tables
con.execute("DELETE FROM dim_news")
con.execute("DELETE FROM dim_companies")
con.execute("DELETE FROM dim_topics")
con.execute("DELETE FROM dim_time")