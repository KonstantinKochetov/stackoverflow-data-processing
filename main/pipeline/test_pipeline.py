from pyspark.sql import SparkSession
import unittest
import logging
from xml_converter import *
from data_manipulator import *
from sql_handler import *

class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        working_directory = '../jars/'
        return (SparkSession.builder
            .master('local[2]')
            .appName('my-local-testing-pyspark-context')
            .config('spark.driver.extraClassPath',
                    working_directory + 'postgresql-42.2.5.jar') \
                .enableHiveSupport()
            .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class SimpleTest(PySparkTest):

    def test_xml_converter(self):
        data_directory = "../data/short_german.stackexchange.com"

        badges = convert_badges(self.spark, data_directory)
        posts = convert_posts(self.spark, data_directory)
        users = convert_users(self.spark, data_directory)

        self.assertEqual(badges.count(), 11)
        self.assertEqual(posts.count(), 74)
        self.assertEqual(users.count(), 238)

    def test_data_manipulator(self):
        data_directory = "../data/short_german.stackexchange.com"

        badges = convert_badges(self.spark, data_directory)
        posts = convert_posts(self.spark, data_directory)
        users = convert_users(self.spark, data_directory)

        final_users = create_new_users_dataframe(self.spark, badges, posts, users)
        self.assertEqual(final_users.count(), 238)
        self.assertEqual(final_users.select('*').where(col('critic') == 1).count(), 4)
        self.assertEqual(final_users.select('*').where(col('editor') == 1).count(), 4)
        self.assertEqual(final_users.select('*').where(col('lastMonthNumberOfPosts') > 0).count() > 0, True)
        self.assertEqual(final_users.select('*').where(col('totalNumberOfPosts') > 0).count() > 0, True)

