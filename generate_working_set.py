#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types

# stolen from CMSSpark
import schemas


def get_df_condor(spark, dates):
    """Condor ClassAd popularity

    Output:
    DataFrame[dataset_name: string, site_name: string, CMS_SubmissionTool: string, timestamp: double]

    timestamp is a UNIX timestamp (seconds)
    """
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField(
                            "DESIRED_CMSDataset", types.StringType(), True
                        ),
                        types.StructField("RecordTime", types.LongType(), True),
                        types.StructField("Site", types.StringType(), True),
                        types.StructField(
                            "CMS_SubmissionTool", types.StringType(), True
                        ),
                        types.StructField("Status", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/condor/raw/metric/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df = (
        df.filter(df.DESIRED_CMSDataset.isNotNull() & (df.Status == "Completed"))
        .drop("Status")
        .withColumn("timestamp", df.RecordTime / fn.lit(1000))
        .drop("RecordTime")
        .withColumnRenamed("DESIRED_CMSDataset", "dataset_name")
        .withColumnRenamed("Site", "site_name")
    )
    return df


def get_df_cmssw(spark, dates):
    """CMSSW UDP collector popularity

    Output:
    DataFrame[file_lfn: string, timestamp: bigint, site_name: string, is_crab: boolean]

    timestamp is UNIX timestamp (seconds)
    """
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField("file_lfn", types.StringType(), True),
                        types.StructField("end_time", types.LongType(), True),
                        types.StructField("app_info", types.StringType(), True),
                        types.StructField("site_name", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/cmssw_pop/raw/metric/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df = (
        df.filter(df.file_lfn.startswith("/store/"))
        .withColumn("is_crab", df.app_info.contains(":crab:"))
        .drop("app_info")
        .withColumnRenamed("end_time", "timestamp")
    )
    return df


def get_df_xrootd(spark, dates):
    """XRootD GLED collector popularity

    Outputs:
    DataFrame[file_lfn: string, client_domain: string, timestamp: double]

    timestamp is UNIX timestamp (seconds)
    """
    schema = types.StructType(
        [
            types.StructField(
                "data",
                types.StructType(
                    [
                        types.StructField("file_lfn", types.StringType(), True),
                        types.StructField("end_time", types.LongType(), True),
                        types.StructField("client_domain", types.StringType(), True),
                        types.StructField("vo", types.StringType(), True),
                    ]
                ),
                False,
            ),
        ]
    )
    df = (
        spark.read.schema(schema)
        .json("/project/monitoring/archive/xrootd/raw/gled/%s/*.json.gz" % dates)
        .select("data.*")
    )
    df = (
        df.filter((df.vo == "cms") & df.file_lfn.startswith("/store/"))
        .drop("vo")
        .withColumn("timestamp", df.end_time / fn.lit(1000))
        .drop("end_time")
        .withColumn(
            "client_domain",
            fn.when(df.client_domain.endswith("]"), None).otherwise(df.client_domain),
        )
    )
    return df


def get_df_wmarchive(spark, dates):
    """WMArchive popularity source

    Outputs:
    DataFrame[site_name: string, file_lfn: string, timestamp: bigint]
    """
    df = spark.read.json(
        "/project/monitoring/archive/wmarchive/raw/metric/%s/*.json.gz" % dates
    ).select("data.*")
    df = df.filter(df.meta_data.jobstate == "success").select(
        df.steps.site[0].alias("site_name"),
        fn.explode(df.LFNArray).alias("file_lfn"),
        df.meta_data.ts.alias("timestamp"),
    )
    df = df.filter(
        df.file_lfn.startswith("/store/") & ~df.file_lfn.startswith("/store/unmerged")
    )
    return df


def run(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.caseSensitive", "true")
    print(
        "Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s"
        % sc.applicationId
    )

    csvreader = (
        spark.read.format("csv").option("nullValue", "null").option("mode", "FAILFAST")
    )
    dbs_files = csvreader.schema(schemas.schema_files()).load(
        "/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/FILES/part-m-00000"
    )
    dbs_datasets = (
        csvreader.schema(schemas.schema_datasets())
        .load("/project/awg/cms/CMS_DBS3_PROD_GLOBAL/current/DATASETS/part-m-00000")
        .withColumn(
            "input_campaign",
            fn.regexp_extract(
                col("d_dataset"),
                r"^/[^/]*/((?:HI|PA|PN|XeXe|)Run201\d\w-[^-]+|CMSSW_\d+|[^-]+)[^/]*/",
                1,
            ),
        )
    )

    if args.source == "classads":
        working_set_day = (
            get_df_condor(spark, args.dates)
            .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
            .join(dbs_datasets, col("dataset_name") == col("d_dataset"))
            .groupBy("day", "input_campaign", "d_data_tier_id")
            .agg(
                fn.collect_set("d_dataset_id").alias("working_set"),
            )
        )
        working_set_day.write.parquet(args.out)
    elif args.source == "cmssw":
        working_set_day = (
            get_df_cmssw(spark, args.dates)
            .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
            .join(dbs_files, col("file_lfn") == col("f_logical_file_name"))
            .join(dbs_datasets, col("f_dataset_id") == col("d_dataset_id"))
            .groupBy("day", "input_campaign", "d_data_tier_id", "site_name", "is_crab")
            .agg(
                fn.collect_set("f_block_id").alias("working_set_blocks"),
            )
        )
        working_set_day.write.parquet(args.out)
    elif args.source == "xrootd":
        working_set_day = (
            get_df_xrootd(spark, args.dates)
            .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
            .join(dbs_files, col("file_lfn") == col("f_logical_file_name"))
            .join(dbs_datasets, col("f_dataset_id") == col("d_dataset_id"))
            .groupBy("day", "input_campaign", "d_data_tier_id", "client_domain")
            .agg(
                fn.collect_set("f_block_id").alias("working_set_blocks"),
            )
        )
        working_set_day.write.parquet(args.out)
    elif args.source == "fwjr":
        working_set_day = (
            get_df_wmarchive(spark, args.dates)
            .withColumn("day", (col("timestamp") - col("timestamp") % fn.lit(86400)))
            .join(dbs_files, col("file_lfn") == col("f_logical_file_name"))
            .join(dbs_datasets, col("f_dataset_id") == col("d_dataset_id"))
            .groupBy("day", "input_campaign", "d_data_tier_id", "site_name")
            .agg(
                fn.collect_set("f_block_id").alias("working_set_blocks"),
            )
        )
        working_set_day.write.parquet(args.out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Computes working set (unique blocks accessed) per day, partitioned by various fields."
    )
    defpath = "hdfs://analytix/user/ncsmith/working_set_day"
    parser.add_argument(
        "--out",
        metavar="OUTPUT",
        help="Output path in HDFS for result (default: %s)" % defpath,
        default=defpath,
    )
    parser.add_argument(
        "--source",
        help="Source",
        default="classads",
        choices=["classads", "cmssw", "xrootd", "fwjr"],
    )
    parser.add_argument("--dates", help="Date portion of file path", default="2020/*/*")

    args = parser.parse_args()
    run(args)
