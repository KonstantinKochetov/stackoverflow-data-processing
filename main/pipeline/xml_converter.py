from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import re
import html


def read_tags_raw(tags_string):  # converts <tag1><tag2> to ['tag1', 'tag2']
    return html.unescape(tags_string).strip('>').strip('<').split('><') if tags_string else []

pattern = re.compile(' ([A-Za-z]+)="([^"]*)"')
parse_line = lambda line: {key: value for key, value in pattern.findall(line)}
unescape = udf(lambda escaped: html.unescape(escaped) if escaped else None)
read_tags = udf(read_tags_raw, ArrayType(StringType()))


def convert_badges(spark, link):
    return spark.read.text(link + '/Badges.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.UserId').cast('integer'),
        col('value.Name'),
        col('value.Date').cast('timestamp'),
        col('value.Class').cast('integer'),
        col('value.TagBased').cast('boolean')
    )

def convert_comments(spark, link):
    return spark.read.text(link + '/Comments.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostId').cast('integer'),  # foreign key
        col('value.Score').cast('integer'),
        unescape('value.Text').alias('Text'),
        col('value.CreationDate').cast('timestamp'),
        col('value.UserId').cast('integer')
    )

def convert_posts(spark, link):
    return spark.read.text(link + '/Posts.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostTypeId').cast('integer'),
        col('value.ParentId').cast('integer'),
        col('value.AcceptedAnswerId').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.Score').cast('integer'),
        col('value.ViewCount').cast('integer'),
        unescape('value.Body').alias('Body'),
        col('value.OwnerUserId').cast('integer'),
        col('value.LastEditorUserId').cast('integer'),
        col('value.LastEditorDisplayName'),
        col('value.LastEditDate').cast('timestamp'),
        col('value.LastActivityDate').cast('timestamp'),
        col('value.CommunityOwnedDate').cast('timestamp'),
        col('value.ClosedDate').cast('timestamp'),
        unescape('value.Title').alias('Title'),
        read_tags('value.Tags').alias('Tags'),
        col('value.AnswerCount').cast('integer'),
        col('value.CommentCount').cast('integer'),
        col('value.FavoriteCount').cast('integer')
    )

def convert_post_history(spark, link):
    return spark.read.text(link + '/PostHistory.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostHistoryTypeId').cast('integer'),
        col('value.PostId').cast('integer'),  # foreign key
        col('value.RevisionGUID'),
        col('value.CreationDate').cast('timestamp'),
        col('value.UserId').cast('integer'),
        col('value.UserDisplayName'),
        unescape('value.Comment').alias('Comment'),
        unescape('value.Text').alias('Text'),
        col('value.CloseReasonId').cast('integer')
    )


def convert_post_links(spark, link):
    return spark.read.text(link + '/PostLinks.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.PostId').cast('integer'),
        col('value.RelatedPostId').cast('integer'),
        col('value.LinkTypeId').cast('integer')
    )


def convert_users(spark, link):
    return spark.read.text(link + '/Users.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.Reputation').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.DisplayName'),
        col('value.EmailHash').cast('integer'),
        col('value.LastAccessDate').cast('timestamp'),
        col('value.WebsiteUrl'),
        col('value.Location'),
        col('value.Age').cast('integer'),
        unescape('value.AboutMe').alias('AboutMe'),
        col('value.Views').cast('integer'),
        col('value.UpVotes').cast('integer'),
        col('value.DownVotes').cast('integer'),
        col('value.ProfileImageUrl'),
        col('value.AccountId').cast('integer')
    )


def convert_votes(spark, link):
    return spark.read.text(link + '/Votes.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.PostId').cast('integer'),
        col('value.VoteTypeId').cast('integer'),
        col('value.CreationDate').cast('timestamp'),
        col('value.UserId').cast('integer'),
        col('value.BountyAmount').cast('integer')
    )

def convert_tags(spark, link):
    return spark.read.text(link + '/Tags.xml').where(col('value').like('%<row Id%')) \
        .select(udf(parse_line, MapType(StringType(), StringType()))('value').alias('value')) \
        .select(
        col('value.Id').cast('integer'),
        col('value.TagName'),
        col('value.Count').cast('integer'),
        col('value.ExcerptPostId').cast('integer'),
        col('value.WikiPostId').cast('integer')
    )

def persist_dataframe_into_parquet(dataframe, output_path):
    dataframe.repartition(5).write.parquet(output_path)

