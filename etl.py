from pyspark.sql import SparkSession
import pyspark.sql.types as t

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
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
        .json(song_data, song_schema)
    
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
    
    
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    input_data = '/home/workspace/data/more/'
    output_data = '/home/workspace/output/'
    
    process_song_data(spark, input_data, output_data)    


if __name__ == "__main__":
    main()