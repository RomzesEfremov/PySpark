from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, trim

from config import host, password, user, db_name

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('SparkTest') \
    .config("spark.driver.extraClassPath", "C:/spark/postgresql-42.7.1.jar") \
    .getOrCreate()

df = (spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("1.csv")
      )



# df.printSchema()

# test_csv = (
#     df.select(col('DESCR').alias('descr'), col('FIO').alias('fio'), col('DataRogdenia').alias('data_rogdenia'),
#               col('TelKont').alias('tel_kont'), col('DataSozdania').alias('data_sozdania'),
#               col('DenRoghdenia').alias('den_roghdenia'), col('ProcentSkidki').alias('procent_skidki'))
# )
# test_csv.select("descr", "fio", "data_rogdenia", regexp_replace("tel_kont", ), "data_sozdania" , "den_roghdenia", "procent_skidki") .show()
# test_csv.select(regexp_extract('tel_kont', r'(?:\+|\d)[\d\-\(\) ]{9,}\d', 0)).show()


kb_sort_df = (df.select(col('DESCR').alias('descr')
                      , col('FIO').alias('fio')
                      , col('DataRogdenia').alias('data_rogdenia')
                      , trim(col('TelKont')).alias('tel_kont')
                      , col('DataSozdania').alias('data_sozdania')
                      , col('DenRoghdenia').alias('den_roghdenia')
                      , col('ProcentSkidki').alias('procent_skidki'))
            ).filter(col('tel_kont') == regexp_extract('tel_kont', r'^((8|(\+7))[\- ]?)(\(?\d{3}\)?[\- ]?)?[\d\- ]{7,10}$', 0))



kb_sort_df.write\
 .format("jdbc")\
 .mode("overwrite")\
 .option("url", "jdbc:postgresql://5.104.75.54/de_roman")\
 .option("dbtable", "kb_sort")\
 .option("user", "roman")\
 .option("password", "0p9o8i7u6y—")\
 .save()

