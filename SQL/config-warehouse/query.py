import duckdb

database_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb'
con = duckdb.connect(database=database_path)  # Use ':memory:' for in-memory database





con.sql("Select * from dim_companies;").show()
con.sql("Select count(*) from dim_companies;").show()
con.sql("Select * from dim_time;").show()
con.sql("Select count(*) from dim_time;").show()
con.sql("Select * from dim_news;").show()
con.sql("Select count(*) from dim_news;").show()
con.sql("Select * from dim_topics;").show()
con.sql("Select count(*) from dim_topics;").show()
con.sql("Select * from fact_candles;").show()
con.sql("Select count(*) from fact_candles;").show()
con.sql("Select * from fact_news_companies;").show()
con.sql("Select count(*) from fact_news_companies;").show()
con.sql("Select * from fact_news_topics;").show()
con.sql("Select count(*) from fact_news_topics;").show()
