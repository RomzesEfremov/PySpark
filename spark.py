from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_extract, trim
import Config

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('SparkTest') \
    .config("spark.driver.extraClassPath", "./Libs/postgresql-42.7.1.jar") \
    .getOrCreate()


def read_data() -> DataFrame:
    print("Reading data...")
    df = (spark.read.format("csv")
          .option("sep", ";")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(Config.get_sours_path())
          )
    return df


def transform_data(df: DataFrame) -> DataFrame:
    print("Transforming data...")
    df = (df.select(col('DESCR').alias('descr')
                    , col('FIO').alias('fio')
                    , col('DataRogdenia').alias('data_rogdenia')
                    , trim(col('TelKont')).alias('tel_kont')
                    , col('DataSozdania').alias('data_sozdania')
                    , col('DenRoghdenia').alias('den_roghdenia')
                    , col('ProcentSkidki').alias('procent_skidki'))
          ).filter(
        col('tel_kont') == regexp_extract('tel_kont', r'^((8|(\+7))[\- ]?)(\(?\d{3}\)?[\- ]?)?[\d\- ]{7,10}$', 0))
    return df


def write_data(df: DataFrame):
    print("Writing data...")
    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", Config.db_url()) \
        .option("dbtable", Config.db_mane()) \
        .option("user", Config.get_name()) \
        .option("password", Config.get_password()) \
        .save()


if __name__ == "__main__":
    kb_raw_data_df = read_data()
    transformed_df = transform_data(kb_raw_data_df)
    write_data(transformed_df)
