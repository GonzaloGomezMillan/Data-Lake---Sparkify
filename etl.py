import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.types as TS



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3://udacity-dend/song_data'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select('song_id','title','artist_id','year','duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode('overwrite').parquet()

    # extract columns to create artists table
    artists_table = song_df.select('artist_id','artist_name','artist_location',
                                   'artist_latitude','artist_longitude')
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet()


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3://udacity-dend/log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level')
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TS.TimestampType())
    df = df.withColumn('ts_timestamp',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
    # extract columns to create time table
    time_table = df.select('ts_timestamp', hour('ts_timestamp').alias('hour'), dayofmonth('ts_timestamp').alias('day'), weekofyear('ts_timestamp').alias('weekofyear'),
                           month('ts_timestamp').alias('month'), year('ts_timestamp').alias('year'), dayofweek('ts_timestamp').alias('dayofweek'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet()

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df['song'] == song_df['title']).drop('title')
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id()).select('songplay_id','ts_timestamp', 'userId', 'level',
                                                                                                      'song_id', 'artist_id', 'sessionId', 'location', 'userAgent')
    # create year and month columns for the partition 
    songplays_table = songplays_table.withColumn('year', year('ts_timestamp')).withColumn('month', month('ts_timestamp'))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet()


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
