
def write_to_sql(properties, dataframe):

    dataframe.write \
        .format('jdbc') \
        .option('driver', properties['driver']) \
        .option('url', properties['url']) \
        .option('user', properties['user']) \
        .option('password', properties['password']) \
        .option('dbtable', properties['dbtable']) \
        .mode('append') \
        .save()

def read_from_sql(spark, properties):

    return spark.read \
        .format('jdbc') \
        .option('driver', properties['driver']) \
        .option('url', properties['url']) \
        .option('user', properties['user']) \
        .option('password', properties['password']) \
        .option('dbtable', properties['dbtable']) \
        .load()

