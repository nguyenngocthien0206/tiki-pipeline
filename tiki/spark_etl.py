from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from datetime import datetime, timedelta

def dim_author(spark):
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "author") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_author.parquet")
    
def dim_seller(spark):
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "seller") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    df = df.select('seller_id','seller_name')
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_seller.parquet")
    
def dim_category(spark):
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "category") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_category.parquet")

def dim_customer(spark):
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "customer") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    df = df.select('customer_id','customer_name')
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_customer.parquet")
    
def dim_date(spark):
    begin_date = datetime.strptime('2000-01-01','%Y-%m-%d')
    end_date   = (datetime.now() + timedelta(days=365 * 20)).replace(month=12, day=31)
    column_rule_df = spark.createDataFrame([
        ("date_id",              "cast(date_format(date, 'yyyyMMdd') as int)"),     # 20230101
        ("year",                "year(date)"),                                     # 2023
        ("quarter",             "quarter(date)"),                                  # 1
        ("month",               "month(date)"),                                    # 1
        ("day",                 "day(date)"),                                      # 1
        ("week",                "weekofyear(date)"),                               # 1
        ("quarter_name_long",     "date_format(date, 'QQQQ')"),                      # 1st qaurter
        ("quarter_name_short",    "date_format(date, 'QQQ')"),                       # Q1
        ("quarter_number_string", "date_format(date, 'QQ')"),                        # 01
        ("month_name_long",       "date_format(date, 'MMMM')"),                      # January
        ("month_name_short",      "date_format(date, 'MMM')"),                       # Jan
        ("month_number_string",   "date_format(date, 'MM')"),                        # 01
        ("day_number_string",     "date_format(date, 'dd')"),                        # 01
        ("week_name_long",        "concat('week', lpad(weekofyear(date), 2, '0'))"), # week 01
        ("week_name_short",       "concat('w', lpad(weekofyear(date), 2, '0'))"),    # w01
        ("week_number_string",    "lpad(weekofyear(date), 2, '0')"),                 # 01
        ("day_of_week",           "dayofweek(date)"),                                # 1
        ("year_month_string",     "date_format(date, 'yyyy/MM')"),                   # 2023/01
        ("day_of_week_name_long",   "date_format(date, 'EEEE')"),                      # Sunday
        ("day_of_week_name_short",  "date_format(date, 'EEE')"),                       # Sun
        ("day_of_month",          "cast(date_format(date, 'd') as int)"),            # 1
        ("day_of_year",           "cast(date_format(date, 'D') as int)")], ["new_column_name", "expression"])
    start = int(begin_date.timestamp())
    stop  = int(end_date.timestamp())
    df = spark.range(start, stop, 60*60*24).select(col("id").cast("timestamp").cast("date").alias("Date"))
    
    for row in column_rule_df.collect():
        new_column_name = row["new_column_name"]
        expression = expr(row["expression"])
        df = df.withColumn(new_column_name, expression)
        
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_date.parquet")

def format_time(time_value):
    return "{:02d}{:02d}{:02d}".format(time_value[0], time_value[1], time_value[2])

def generate_time_range(start_hour, end_hour):
    for hour in range(start_hour, end_hour + 1):
        for minute in range(0, 60):
            for second in range(0, 60):
                yield (hour, minute, second)
    
def dim_time(spark):
    schema = ['hour','minute','second']
    format_udf = udf(format_time)
    time_range_df = spark.sparkContext.parallelize(generate_time_range(0,23)).toDF(schema)
    df = time_range_df.withColumn("time_id", format_udf(array("hour", "minute", "second")))
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_time.parquet")
    
def dim_product(spark):
    product_df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "product") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    written_df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "written") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    written_df = written_df.withColumnRenamed('product_id','written_product_id')
        
    product_df = product_df.select('product_id','product_name','category_id','seller_id','original_price','quantity_sold','rating_average','review_count')
    temp_df = product_df.join(written_df,product_df.product_id==written_df.written_product_id,"left")
    df = temp_df.select('product_id','product_name','category_id','seller_id','author_id','original_price','quantity_sold','rating_average','review_count')
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/dim_product.parquet")

def fact_review(spark):
    df = spark.read \
        .format("jdbc") \
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", "jdbc:mysql://localhost:3306/tiki") \
        .option("dbtable", "review") \
        .option("user", "thien") \
        .option("password", "thien") \
        .load()
    df = df.filter(col("customer_id").isNotNull() & col("product_id").isNotNull())
    df = df.withColumn('date_id',date_format(col('created_at'), 'yyyyMMdd').cast('int'))
    df = df.withColumn('hour',hour(col('created_at')))
    df = df.withColumn('minute',minute(col('created_at')))
    df = df.withColumn('second',second(col('created_at')))
    format_udf = udf(format_time)
    df = df.withColumn("time_id", format_udf(array("hour", "minute", "second")))
    df = df.select('review_id','product_id','customer_id','date_id','time_id','rating','thank_count','title','content')
    df.write.mode('append').parquet("hdfs://localhost:9000/user/thien/tiki/dwh/fact_review.parquet")
    
def main():
    spark = SparkSession.builder.appName("TikiETL").getOrCreate()
    dim_author(spark)
    dim_seller(spark)
    dim_category(spark)
    dim_customer(spark)
    dim_date(spark)
    dim_time(spark)
    dim_product(spark)
    fact_review(spark)
    
    
if __name__ == '__main__':
    main()