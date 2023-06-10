"""
Author : Soumil Nitin Shah
Email shahsoumil519@gmail.com
"""

try:
    import sys, os, ast, uuid, boto3, datetime, time, re, json
    from ast import literal_eval
    from dataclasses import dataclass
    from datetime import datetime
    from pyspark.sql.functions import lit, udf
    from pyspark.sql.types import StringType
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.types import *
    import pandas as pd

except Exception as e:
    print("Modules are missing : {} ".format(e))

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME'
    ],
)

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)
global BUCKET_NAME, loaders_json_payload

loaders_json_payload = {
    "check_point_buckets": "sXXXX",
    "source": [
        {
            "source_type": "HUDI",  # HUDI | DYNAMODB
            "table_name": "orders",
            "spark_table_name": "orders",  # Creates TempView with this table name
            "path": "s3://sXXXXXXsilver/table_name=orders/",
            "type": "FULL"  # INC | FULL
        },
        {
            "source_type": "HUDI",  # HUDI | DYNAMODB
            "table_name": "customers",
            "spark_table_name": "customers",  # Creates TempView with this table name
            "path": "s3://XXXXXXXsilver/table_name=customers/",
            "type": "FULL"  # INC | FULL
        },
    ]
}
BUCKET_NAME = loaders_json_payload.get("check_point_buckets")


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket):

        self.BucketName = bucket
        self.client = boto3.client("s3")

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:
            response = self.client.put_object(
                Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


@dataclass
class HUDISettings:
    """Class for keeping track of an item in inventory."""

    table_name: str
    path: str


class HUDIIncrementalReader(AWSS3):
    def __init__(self, bucket, hudi_settings, spark_session):
        AWSS3.__init__(self, bucket=bucket)
        if type(hudi_settings).__name__ != "HUDISettings": raise Exception("please pass correct settings ")
        self.hudi_settings = hudi_settings
        self.spark = spark_session

    def __check_meta_data_file(self):
        """
        check if metadata for table exists
        :return: Bool
        """
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        return self.item_exists(Key=file_name)

    def __read_meta_data(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"

        return ast.literal_eval(self.get_item(Key=file_name).decode("utf-8"))

    def __push_meta_data(self, json_data):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.put_files(
            Key=file_name, Response=json.dumps(json_data)
        )

    def clean_check_point(self):
        file_name = f"metadata/{self.hudi_settings.table_name}.json"
        self.delete_object(Key=file_name)

    def __get_begin_commit(self):
        self.spark.read.format("hudi").load(self.hudi_settings.path).createOrReplaceTempView("hudi_snapshot")
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_snapshot order by commitTime asc").limit(
            50).collect()))

        """begin from start """
        begin_time = int(commits[0]) - 1
        return begin_time

    def __read_inc_data(self, commit_time):
        incremental_read_options = {
            'hoodie.datasource.query.type': 'incremental',
            'hoodie.datasource.read.begin.instanttime': commit_time,
        }
        incremental_df = self.spark.read.format("hudi").options(**incremental_read_options).load(
            self.hudi_settings.path).createOrReplaceTempView("hudi_incremental")

        df = self.spark.sql("select * from  hudi_incremental")

        return df

    def __get_last_commit(self):
        commits = list(map(lambda row: row[0], self.spark.sql(
            "select distinct(_hoodie_commit_time) as commitTime from  hudi_incremental order by commitTime asc").limit(
            50).collect()))
        last_commit = commits[len(commits) - 1]
        return last_commit

    def __run(self):
        """Check the metadata file"""
        flag = self.__check_meta_data_file()
        """if metadata files exists load the last commit and start inc loading from that commit """
        if flag:
            meta_data = json.loads(self.__read_meta_data())
            print(f"""
            ******************LOGS******************
            meta_data {meta_data}
            last_processed_commit : {meta_data.get("last_processed_commit")}
            ***************************************
            """)

            read_commit = str(meta_data.get("last_processed_commit"))
            df = self.__read_inc_data(commit_time=read_commit)

            """if there is no INC data then it return Empty DF """
            if not df.rdd.isEmpty():
                last_commit = self.__get_last_commit()
                self.__push_meta_data(json_data=json.dumps({
                    "last_processed_commit": last_commit,
                    "table_name": self.hudi_settings.table_name,
                    "path": self.hudi_settings.path,
                    "inserted_time": datetime.now().__str__(),

                }))
                return df
            else:
                return df

        else:

            """Metadata files does not exists meaning we need to create  metadata file on S3 and start reading from begining commit"""

            read_commit = self.__get_begin_commit()

            df = self.__read_inc_data(commit_time=read_commit)
            last_commit = self.__get_last_commit()

            self.__push_meta_data(json_data=json.dumps({
                "last_processed_commit": last_commit,
                "table_name": self.hudi_settings.table_name,
                "path": self.hudi_settings.path,
                "inserted_time": datetime.now().__str__(),

            }))

            return df

    def read(self):
        """
        reads INC data and return Spark Df
        :return:
        """

        return self.__run()


def upsert_hudi_table(
        db_name,
        table_name,
        record_id,
        precomb_key,
        spark_df,
        table_type='COPY_ON_WRITE',
        method='upsert',

):
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,

    }

    hudi_hive_sync_options = {
        "hoodie.datasource.hive_sync.database": db_name,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",

    }

    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true"
        , "hoodie.clean.async": "true"
        , "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS'
        , "hoodie.cleaner.fileversions.retained": "3"
        , "hoodie-conf hoodie.cleaner.parallelism": '200'
        , 'hoodie.cleaner.commits.retained': 5

    }

    for key, value in hudi_hive_sync_options.items(): hudi_final_settings[key] = value
    earning_fact = f"s3://{BUCKET_NAME}/gold/table_name=driver_earnings/"

    spark_df.write.format("hudi"). \
        options(**hudi_final_settings). \
        mode("append"). \
        save(earning_fact)


def get_spark_df_from_dynamodb_table(dynamodb_table):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamodb_table)
    response = table.scan()

    # Convert DynamoDB items to a list of dictionaries
    items = response['Items']

    # Create a Spark DataFrame from the list of dictionaries
    spark_df = spark.createDataFrame(items)
    return spark_df


def load_hudi_tables(loaders):
    """load Hudi tables """

    for items in loaders.get("source"):
        table_name = items.get("table_name")
        path = items.get("hudi_path")

        if items.get("type") == "FULL":
            spark.read.format("hudi").load(path).createOrReplaceTempView(table_name)

        if items.get("type") == "INC":
            helper = HUDIIncrementalReader(
                bucket=BUCKET_NAME,
                hudi_settings=HUDISettings(
                    table_name=table_name,
                    path=path
                ),
                spark_session=spark
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(table_name)


class HudiLoader(object):
    def __init__(self,
                 table_name,
                 hudi_path,
                 spark_session,
                 check_points_bucket,
                 spark_table_name
                 ):
        """
        Based on type it will load the Hudi Tables
        :param table_name: str
        :param hudi_path: str
        :param spark_session: obj
        """
        self.__table_name = table_name
        self.__hudi_path = hudi_path
        self.__type = type
        self.__spark_session = spark_session
        self.__spark_table_name = spark_table_name
        self.__check_points_bucket = check_points_bucket

    def incremental_load(self):
        """
        Creates and load the Incremental data
        :return: Bool
        """
        try:

            helper = HUDIIncrementalReader(
                bucket=self.__check_points_bucket,
                hudi_settings=HUDISettings(
                    table_name=self.__table_name,
                    path=self.__hudi_path
                ),
                spark_session=self.__spark_session
            )
            spark_df = helper.read()
            spark_df.createOrReplaceTempView(self.__spark_table_name)
            return True
        except Exception as e:
            print(f"Error loading Table {self.__table_name} {e}")
            return False

    def full_load(self):
        try:
            self.__spark_session.read.format("hudi").load(self.__hudi_path).createOrReplaceTempView(
                self.__spark_table_name)
            return True
        except Exception as e:
            print(f"Error loading Table {self.__table_name} {e}")
            return False


class DynamoDBLoader(object):
    @staticmethod
    def get_spark_df_from_dynamodb_table(dynamodb_table, spark_session, spark_table_name):
        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(dynamodb_table)
            response = table.scan()

            # Convert DynamoDB items to a list of dictionaries
            items = response['Items']

            # Create a Spark DataFrame from the list of dictionaries
            spark_df = spark_session.createDataFrame(items)
            spark_df.createOrReplaceTempView(spark_table_name)
            return True

        except Exception as e:
            print("Error Creating or Loading DynamoDB")
            return False


class Loaders(object):
    def __init__(self, json_payload, spark_session):
        self.json_payload = json_payload
        self.spark_session = spark_session

    def load(self):
        for item in self.json_payload.get("source"):
            if item.get("source_type") == "HUDI":
                helper = HudiLoader(
                    hudi_path=item.get("path"),
                    table_name=item.get("table_name"),
                    check_points_bucket=self.json_payload.get("check_point_buckets"),
                    spark_session=self.spark_session,
                    spark_table_name=item.get("spark_table_name")
                )
                if item.get("type") == "INC":
                    helper.incremental_load()

                if item.get("type") == "FULL":
                    helper.full_load()

            if item.get("source_type") == "DYNAMODB":
                response = DynamoDBLoader.get_spark_df_from_dynamodb_table(
                    dynamodb_table=item.get("table_name"),
                    spark_session=self.spark_session,
                    spark_table_name=item.get("spark_table_name")
                )


from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute


class CustomerModel(Model):
    class Meta:
        table_name = 'orders'
        region = 'us-east-1'  # Replace with your desired AWS region

    customer_id = UnicodeAttribute(hash_key=True)
    order_id = UnicodeAttribute(range_key=True)

    customer_name =UnicodeAttribute(null=True)
    state = UnicodeAttribute(null=True)
    city = UnicodeAttribute(null=True)
    email = UnicodeAttribute(null=True)
    order_value = UnicodeAttribute(null=True)
    order_date = UnicodeAttribute(null=True)
    order_name = UnicodeAttribute(null=True)


def main():
    try:
        CustomerModel.create_table(billing_mode='PAY_PER_REQUEST')
    except Exception as e:
        pass

    helper = Loaders(
        json_payload=loaders_json_payload,
        spark_session=spark
    )
    helper.load()

    query = """
    SELECT  c.customer_id,
            o.order_id,
            c.city,
            c.name AS customer_name,
            c.email,
            o.name AS order_name,
            o.order_value,
            o.order_date
    FROM 
        customers c
    JOIN 
        orders o ON c.customer_id = o.customer_id
    """

    df_result = spark.sql(query)
    df_result.show()

    # Convert Spark DataFrame to Pandas DataFrame
    pandas_df = df_result.toPandas()

    for _, row in pandas_df.iterrows():
        customer_id = row['customer_id']
        order_id = row['order_id']
        customer_name = str(row['customer_name'])
        email = str(row['email'])
        order_name = str(row['order_name'])
        order_value = str(row['order_value'])
        order_date = str(row['order_date'])

        # Create an instance of CustomerModel and save it to DynamoDB
        customer = CustomerModel(
            customer_id=customer_id,
            order_id=order_id,
            customer_name=customer_name,
            email=email,
            order_name=order_name,
            order_value=order_value,
            order_date=order_date
        )
        customer.save()


main()
