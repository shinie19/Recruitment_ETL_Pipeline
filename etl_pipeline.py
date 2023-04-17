from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time
import uuid
import time_uuid

CASSANDRA_HOST = "etl_cassandra"
CASSANDRA_KEYSPACE = "recruitment"
CASSANDRA_TABLE = "tracking"

MYSQL_HOST = "etl_mysql"
MYSQL_DATABASE = "recruitment"
MYSQL_ROOT_PASSWORD = "123456"


def read_data_from_cassandra(keyspace_name, table_name):
    spark = SparkSession.builder \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST).getOrCreate()

    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace_name) \
        .load()
    return df


def read_data_from_mysql(db_name, table_name):
    spark = SparkSession.builder.getOrCreate()

    jdbcUrl = "jdbc:mysql://{host}:3306/{db}".format(
        host=MYSQL_HOST, db=MYSQL_DATABASE)
    connectionProperties = {
        "user": "root",
        "password": "123456"
    }

    df = spark.read.jdbc(url=jdbcUrl, table=table_name,
                         properties=connectionProperties)

    return df


def write_data_to_mysql(db_name, table_name, dataframe):
    jdbcUrl = "jdbc:mysql://{host}:3306/{db}".format(
        host=MYSQL_HOST, db=db_name)
    connectionProperties = {
        "user": "root",
        "password": "123456"
    }

    try:
        dataframe.write.jdbc(url=jdbcUrl, table=table_name,
                             mode="append", properties=connectionProperties)
        return True
    except:
        return False


def convertUUIDToDatetime(timeUUID):
    my_uuid = uuid.UUID(timeUUID)
    datetime = time_uuid.TimeUUID(
        bytes=my_uuid.bytes).get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    return datetime


convertUUIDToDatetimeUDF = udf(
    lambda z: convertUUIDToDatetime(z), StringType())


def handle_datetime(dataframe):
    df = dataframe.withColumn(
        "ts", convertUUIDToDatetimeUDF(col("create_time")))

    df = df.select("job_id", "ts", "custom_track", "bid", "campaign_id",
                   "group_id", "publisher_id").filter(df.job_id.isNotNull())
    df = df.withColumn("dates", to_date("ts")).withColumn(
        "hours", hour("ts")).drop("ts")
    return df


def handle_custom_track(dataframe, custom_track):
    output_df = dataframe.filter(dataframe.custom_track == custom_track)

    if (custom_track == "click"):
        output_df = output_df.groupBy("job_id", "dates", "hours", "publisher_id", "group_id", "campaign_id") \
            .agg(count("*").alias("clicks"),
                 round(avg("bid"), 2).alias("bid_set"),
                 round(sum("bid"), 2).alias("spend_hour"),
                 )
    else:
        output_df = output_df.groupBy("job_id", "dates", "hours", "publisher_id", "group_id", "campaign_id") \
            .agg(count("*").alias(custom_track))

    return output_df


def read_company_id():
    df = read_data_from_mysql("recruitment", "job").select("id", "company_id")
    df = df.withColumnRenamed("id", "job_id")
    return df


def join_data(click_data, conversion_data, qualified_data, unqualified_data, company_data):
    final_data = click_data.join(conversion_data, ["job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id"], "full") \
        .join(qualified_data, ["job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id"], "full") \
        .join(unqualified_data, ["job_id", "dates", "hours", "publisher_id", "campaign_id", "group_id"], "full") \
        .join(company_data, "job_id")

    final_data = final_data.withColumn("sources", lit("Cassandra"))
    return final_data


def get_latest_time_cassandra():
    df = read_data_from_cassandra("recruitment", "tracking")
    cassandra_time = df.agg(max("ts")).take(
        1)[0][0].strftime('%Y-%m-%d %H:%M:%S')
    return cassandra_time


def get_latest_time_mysql():
    df = read_data_from_mysql("recruitment", "events_etl")
    mysql_time = df.agg(max("latest_modified_time")).take(1)[0][0]
    if mysql_time is None:
        mysql_time = '1998-01-01 23:59:59'
    else:
        mysql_time = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_time


def main_task_etl(mysql_time):
    print("-----------------------------")
    print("Retrieve data from Cassandra")
    print("-----------------------------")
    df = read_data_from_cassandra("recruitment", "tracking")
    df = df.where(col("ts") > mysql_time)
    df = handle_datetime(df)
    # df.show()

    print("-----------------------------")
    print("Process Cassandra statistics")
    print("-----------------------------")
    click_data = handle_custom_track(df, "click")
    conversion_data = handle_custom_track(df, "conversion")
    qualified_data = handle_custom_track(df, "qualified").withColumnRenamed(
        "qualified", "qualified_application")
    unqualified_data = handle_custom_track(df, "unqualified").withColumnRenamed(
        "unqualified", "disqualified_application")

    print("-----------------------------")
    print("Retrieving Company ID")
    print("-----------------------------")
    company_data = read_company_id()

    print("-----------------------------")
    print("Finalizing Output")
    print("-----------------------------")
    final_data = join_data(click_data, conversion_data,
                           qualified_data, unqualified_data, company_data)

    final_data.show()
    status = write_data_to_mysql("recruitment", "events_etl", final_data)

    if (status):
        print("Task finished!")
    else:
        print("Task failed!")


def main():
    while True:
        start = time.time()

        cassandra_time = get_latest_time_cassandra()
        mysql_time = get_latest_time_mysql()

        print("Latest time in Cassandra is: {}".format(cassandra_time))
        print("Latest time in MySQL is: {}".format(mysql_time))
        if (cassandra_time > mysql_time):
            main_task_etl(mysql_time)
        else:
            print("No New data found!")

        end = time.time()
        print("Job takes {} seconds to execute".format(end - start))

        time.sleep(60)


if __name__ == "__main__":
    main()
