from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from datetime import datetime
from pyspark.sql import Window
import configparser
import os

      
def create_spark_session():
    """
    Description: This function can be used to create SparkSession object.

    Arguments:
        None. 

    Returns:
        spark: SparkSession object. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function can be used to process song-data files from the 
    given input path and transform the data from json files into songs and artists 
    spark tables and writing these tables to the given output path as parquet tables.

    Arguments:
        spark: SparkSession object. 
        input_data: Path to the input JSON files. 
        output_data: Path to the output directory that stores output parquet tables.

    Returns:
        df: Song data dataframe. 
    """    
    # get filepath to song data file
    song_data = input_data + 'song-data/A/B/*/*.json'
    
    # define schema for song data file
    song_schema = t.StructType([
        t.StructField("artist_id", t.StringType(), True),
        t.StructField("artist_latitude", t.DecimalType(11, 7), True),
        t.StructField("artist_location", t.StringType(), True),
        t.StructField("artist_longitude", t.DecimalType(11, 7), True),
        t.StructField("artist_name", t.StringType(), True),
        t.StructField("duration", t.DecimalType(11, 7), True),
        t.StructField("num_songs", t.IntegerType(), True),
        t.StructField("song_id", t.StringType(), True),
        t.StructField("title", t.StringType(), True),
        t.StructField("year", t.ShortType(), True)
    ])
    
    # read song data file using schema
    df = spark \
        .read \
        .format("json") \
        .schema(song_schema) \
        .load(song_data)
    
    # extract columns to create songs table
    songs_table = df \
        .select(['song_id', 'title', 'artist_id', 'year', 'duration']) \
        .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist 
    songs_output = output_data + 'songs'
    
    songs_table \
        .write \
        .partitionBy('year', 'artist_id') \
        .option("path", songs_output) \
        .saveAsTable('songs', format='parquet') 
    
    # extract columns to create artists table
    artists_table = df \
        .select(['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']) \
        .dropDuplicates()
    
    # write artists table to parquet files
    artists_output = output_data + 'artists'
    
    artists_table \
        .write \
        .option("path", artists_output) \
        .saveAsTable('artists', format='parquet')
    
    return df
    
def process_log_data(spark, input_data, output_data, song_df):
    """
    Description: This function can be used to process log-data files from the 
    given input path and transform the data from json files into users, time and songplays  
    spark tables and writing these tables to the given output path as parquet tables.

    Arguments:
        spark: SparkSession object. 
        input_data: Path to the input JSON files. 
        output_data: Path to the output directory that stores output parquet tables.
        song_df: Song data dataframe.

    Returns:
        None. 
    """  
    # get filepath to log data file
    log_data = input_data + 'log-data/2018/11'
    
    # define schema for log data file
    log_schema = t.StructType([
        t.StructField("artist", t.StringType(), True),
        t.StructField("auth", t.StringType(), True),
        t.StructField("firstName", t.StringType(), True),
        t.StructField("gender", t.StringType(), True),
        t.StructField("itemInSession", t.IntegerType(), True),
        t.StructField("lastName", t.StringType(), True),
        t.StructField("length", t.DecimalType(12, 7), True),
        t.StructField("level", t.StringType(), True),
        t.StructField("location", t.StringType(), True),
        t.StructField("method", t.StringType(), True),
        t.StructField("page", t.StringType(), True),
        t.StructField("registration", t.DecimalType(16, 2), True),
        t.StructField("sessionId", t.IntegerType(), True),
        t.StructField("song", t.StringType(), True),
        t.StructField("status", t.IntegerType(), True),
        t.StructField("ts", t.LongType(), True),
        t.StructField("userAgent", t.StringType(), True),
        t.StructField("userId", t.StringType(), True)
    ])

    # read log data file using schema
    df = spark \
        .read \
        .format("json") \
        .schema(log_schema) \
        .load(log_data)
        
    # filter by actions for song plays
    df = df \
        .filter('page = "NextSong"')

    # group by userId for unique users    
    users_list = df \
        .groupBy('userId') \
        .agg(f.max('ts').alias('ts'))
    
    # extract columns to create users table
    users_table = df \
        .join(users_list, ['userId', 'ts'], 'inner') \
        .select([df.userId.cast(t.IntegerType()).alias('user_id'), col('firstName').alias('first_name'), col('lastName').alias('last_name'), 'gender', 'level']) \
        .dropDuplicates()
    
    # write users table to parquet files
    users_output = output_data + 'users'
    
    users_table \
        .write \
        .option("path", users_output) \
        .saveAsTable('users', format='parquet')
    
    # create timestamp column from original timestamp column
    df = df \
        .withColumn('timestamp', f.from_utc_timestamp((df.ts/1000.0).cast('timestamp'), 'UTC'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts/1000.0), t.TimestampType())
    df = df.withColumn('datetime', get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df \
        .select([col('datetime').alias('start_time'), dayofmonth(col('datetime')).alias('day'), weekofyear(col('datetime')).alias('week'), month(col('datetime')).alias('month'), year(col('datetime')).alias('year'), dayofweek(col('datetime')).alias('weekday')]) \
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_output = output_data + 'time'
    
    time_table \
        .write \
        .partitionBy('year', 'month') \
        .option("path", time_output) \
        .saveAsTable('time', format='parquet') 
    
    # join and extract columns from song and log datasets to create songplays table 
    cond = [df.artist == song_df.artist_name, df.song == song_df.title, df.length == song_df.duration]
    songplays_df = df.join(song_df, cond, 'left')    
    
    songplays_df = songplays_df \
        .select(df.datetime.alias('start_time'), df.userId.alias('user_id'), df.level.alias('level'), song_df.song_id.alias('song_id'), song_df.artist_id.alias('artist_id'), df.sessionId.alias('session_id'), df.location.alias('location'), df.userAgent.alias('user_agent'), year(df.datetime).alias('year'), month(df.datetime).alias('month'))   
    w = Window().orderBy(f.lit('A'))
    songplays_table = songplays_df.withColumn('songplay_id', f.row_number().over(w))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_output = output_data + 'songplays'
    
    songplays_table \
        .select(['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'year', 'month'])\
        .write \
        .partitionBy('year', 'month') \
        .option("path", songplays_output) \
        .saveAsTable('songplays', format='parquet') 


def main():
    """
    Description: This function can be used to load data from json files in AWS S3
    and process and transform the data into spark analytics tables and write them 
    back into S3.

    Arguments:
        None. 

    Returns:
        None. 
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.access.key", config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID'))
    hadoop_conf.set("fs.s3a.secret.key", config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY'))
    
    input_data = 's3a://udacity-dend/'
    output_data = '/home/workspace/output/'
    
    song_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, song_df)

if __name__ == "__main__":
    main()