from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local")
             .appName("paytm")
             .getOrCreate())

    weather_df = spark.read\
        .option("header", True)\
        .csv("data/2019/*.gz")

    weather_df = weather_df\
        .withColumn('TEMP', F.when(F.col('TEMP').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('TEMP')))\
        .withColumn('DEWP', F.when(F.col('DEWP').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('DEWP')))\
        .withColumn('SLP', F.when(F.col('SLP').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('SLP')))\
        .withColumn('STP', F.when(F.col('STP').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('STP'))) \
        .withColumn('VISIB', F.when(F.col('VISIB').cast(T.DoubleType()) == 0.0, 999.9).otherwise(F.col('VISIB'))) \
        .withColumn('WDSP', F.when(F.col('WDSP').cast(T.DoubleType()) == 0.0, 999.9).otherwise(F.col('WDSP'))) \
        .withColumn('MXSPD', F.when(F.col('MXSPD').cast(T.DoubleType()) == 0.0, 999.9).otherwise(F.col('MXSPD'))) \
        .withColumn('GUST', F.when(F.col('GUST').cast(T.DoubleType()) == 0.0, 999.9).otherwise(F.col('GUST'))) \
        .withColumn('MAX', F.when(F.col('MAX').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('MAX'))) \
        .withColumn('MIN', F.when(F.col('MIN').cast(T.DoubleType()) == 0.0, 9999.9).otherwise(F.col('MIN'))) \
        .withColumn('PRCP', F.when(F.col('PRCP').cast(T.DoubleType()) == 0.0, 99.9).otherwise(F.col('PRCP'))) \
        .withColumn('SNDP', F.when(F.col('SNDP').cast(T.DoubleType()) == 0.0, 999.9).otherwise(F.col('SNDP')))\

    country_df = spark.read.option("header", True).csv("countrylist.csv")
    station_df = spark.read.option("header", True).csv("stationlist.csv")

    station_country_df = station_df.join(country_df, station_df['COUNTRY_ABBR'] == country_df['COUNTRY_ABBR'])\
        .select(station_df["*"], country_df['COUNTRY_FULL'])

    global_weather_df = weather_df.join(station_country_df, weather_df['STN---'] == station_country_df['STN_NO']) \
        .select(weather_df["*"], station_country_df['COUNTRY_FULL'])
    global_weather_df = global_weather_df.withColumn("year", F.year(F.to_date(F.col('YEARMODA'), "yyyyMMdd")))

    #global_weather_df.show(10)
    hotest_avgtmp_df = global_weather_df\
        .groupby('year', 'COUNTRY_FULL')\
        .agg(F.avg(F.col('TEMP')).alias('avg_temp'))\
        .sort(F.col('avg_temp').desc())\
        .limit(1)
    hotest_avgtmp_df.show()

    funnel_cloud_df = global_weather_df \
        .groupby('year', 'COUNTRY_FULL') \
        .agg(F.max(F.col('FRSHTT')).alias('funnel_cloud')) \
        .sort(F.col('funnel_cloud').desc()) \
        .limit(1)
    funnel_cloud_df.show()

    windspeed_df = global_weather_df \
        .groupby('year', 'COUNTRY_FULL') \
        .agg(F.avg(F.col('WDSP')).alias('wind_speed')) \
        .sort(F.col('wind_speed').desc()) \
        .limit(2)
    windspeed_df = windspeed_df.sort(F.col('wind_speed').asc()).limit(1)
    windspeed_df.show()



