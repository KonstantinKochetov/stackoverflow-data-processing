from pyspark.sql.session import SparkSession
from xml_converter import *
from data_manipulator import *
from sql_handler import *

working_directory = '../jars/'
# initialize
spark = (SparkSession.builder
         .appName('stackoverflow')
         .config('spark.driver.extraClassPath',
                 working_directory + 'postgresql-42.2.5.jar') \
         .getOrCreate())

data_directory = "../data/short_german.stackexchange.com"

# parsing data
badges = convert_badges(spark, data_directory)
posts = convert_posts(spark, data_directory)
users = convert_users(spark, data_directory)

# creating additional columns
final_users = create_new_users_dataframe(spark, badges, posts, users)

# properties of the remote sql server
# TODO pass your database properties here
properties = {
    'driver': 'org.postgresql.Driver',
    'url': 'jdbc:postgresql:stackoverflow',
    'user': 'postgres',
    'password': 'testpassword',
    'dbtable': 'users',
}

# write/read to remote sql database
write_to_sql(properties, final_users)
df = read_from_sql(spark, properties)

# show some results
df.show(3)
print(df.count())







