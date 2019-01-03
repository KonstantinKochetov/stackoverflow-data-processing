from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_new_users_dataframe(spark, badges, posts, users):
    badges.createOrReplaceTempView("badges_sql")
    posts.createOrReplaceTempView("posts_sql")
    users.createOrReplaceTempView("users_sql")

    new_users_1 = add_total_number_of_posts(spark)
    new_users_2 = add_critic_status(spark, new_users_1)
    new_users_3 = add_editor_status(spark, new_users_2)
    final_users = add_last_month_number_of_posts(spark, new_users_3)
    return final_users


def add_total_number_of_posts(spark):
    # create table with number of posts per user
    posts_number = spark.sql(
        "SELECT tmp.ownerUserId, COUNT(*) as totalNumberOfPosts"
        " FROM (posts_sql join users_sql"
        "     on users_sql.id == posts_sql.ownerUserId) as tmp"
        " GROUP BY tmp.ownerUserId"
    )

    posts_number.createOrReplaceTempView("posts_number_sql")

    # JOIN with users table -> users table with number of posts
    new_users_1 = spark.sql(
        " SELECT tmp.id, tmp.reputation, tmp.creationDate, tmp.displayName, tmp.emailHash,"
        " tmp.lastAccessDate, tmp.websiteUrl, tmp.location, tmp.age, tmp.aboutMe, tmp.views,"
        " tmp.upVotes, tmp.downVotes, tmp.profileImageUrl, tmp.accountId, COALESCE(tmp.totalNumberOfPosts, 0) as totalNumberOfPosts"
        " FROM (users_sql LEFT OUTER JOIN posts_number_sql"
        "  on users_sql.id == posts_number_sql.ownerUserId) as tmp")

    return new_users_1


def add_critic_status(spark, new_users_1):
    new_users_1.createOrReplaceTempView("new_users_1_sql")

    # table with critics
    critics = spark.sql(
        "SELECT tmp.userId, tmp.name as badge"
        " FROM (badges_sql join users_sql"
        " on badges_sql.userId == users_sql.id) as tmp"
        " WHERE tmp.name == 'Critic'"
    )

    critics = critics.withColumn("critic", when(critics.badge == "Critic", 1).otherwise(0)).drop(
        col("badge"))
    critics.createOrReplaceTempView("critics_sql")

    new_users_2 = spark.sql(
        " SELECT tmp.id, tmp.reputation, tmp.creationDate, tmp.displayName, tmp.emailHash,"
        " tmp.lastAccessDate, tmp.websiteUrl, tmp.location, tmp.age, tmp.aboutMe, tmp.views,"
        " tmp.upVotes, tmp.downVotes, tmp.profileImageUrl, tmp.accountId, tmp.totalNumberOfPosts,"
        " COALESCE(tmp.critic, 0) as critic"
        " FROM (new_users_1_sql LEFT OUTER JOIN critics_sql"
        "  on new_users_1_sql.id == critics_sql.userId) as tmp")

    return new_users_2


def add_editor_status(spark, new_users_2):
    new_users_2.createOrReplaceTempView("new_users_2_sql")

    # table with editors
    editors = spark.sql("SELECT tmp.userId, tmp.name as badge"
                        " FROM (badges_sql join users_sql"
                        " on badges_sql.userId == users_sql.id) as tmp"
                        " WHERE tmp.name == 'Editor'"
                        )

    editors = editors.withColumn("editor", when(editors.badge == "Editor", 1).otherwise(0)).drop(
        col("badge"))
    editors.createOrReplaceTempView("editors_sql")

    new_users_3 = spark.sql(
        " SELECT tmp.id, tmp.reputation, tmp.creationDate, tmp.displayName, tmp.emailHash,"
        " tmp.lastAccessDate, tmp.websiteUrl, tmp.location, tmp.age, tmp.aboutMe, tmp.views,"
        " tmp.upVotes, tmp.downVotes, tmp.profileImageUrl, tmp.accountId, tmp.totalNumberOfPosts,"
        " tmp.critic, COALESCE(tmp.editor, 0) as editor"
        " FROM (new_users_2_sql LEFT OUTER JOIN editors_sql"
        "  on new_users_2_sql.id == editors_sql.userId) as tmp")

    return new_users_3


def add_last_month_number_of_posts(spark, new_users_3):
    new_users_3.createOrReplaceTempView("new_users_3_sql")

    # table with last 30 days number ofposts
    number_posts_30_days = spark.sql("SELECT p.ownerUserId, COUNT(*) as lastMonthNumberOfPosts"
                                     " FROM posts_sql as p"
                                     " WHERE (CURRENT_TIMESTAMP - INTERVAL 30 days) <= p.creationDate"
                                     " GROUP BY p.ownerUserId")

    number_posts_30_days.createOrReplaceTempView("number_posts_30_days_sql")

    final_users = spark.sql(
        " SELECT tmp.id, tmp.reputation, tmp.creationDate, tmp.displayName, tmp.emailHash,"
        " tmp.lastAccessDate, tmp.websiteUrl, tmp.location, tmp.age, tmp.aboutMe, tmp.views,"
        " tmp.upVotes, tmp.downVotes, tmp.profileImageUrl, tmp.accountId, tmp.totalNumberOfPosts,"
        " tmp.critic, tmp.editor, COALESCE(tmp.lastMonthNumberOfPosts,0) as lastMonthNumberOfPosts"
        " FROM (new_users_3_sql LEFT OUTER JOIN number_posts_30_days_sql"
        "  on new_users_3_sql.id == number_posts_30_days_sql.ownerUserId) as tmp")

    return final_users
