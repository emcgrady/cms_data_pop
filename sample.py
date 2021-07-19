#!/usr/bin/env spark-submit
from __future__ import print_function
import argparse
import json
import os

from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col
import pyspark.sql.functions as fn
import pyspark.sql.types as types


def sample(args):
    conf = SparkConf().setMaster("yarn").setAppName("CMS Working Set")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    print("Initiated spark session on yarn, web URL: http://ithdp1101.cern.ch:8088/proxy/%s" % sc.applicationId)

    formats = {
        'avro': spark.read.format("com.databricks.spark.avro").load,
        'csv': spark.read.format("com.databricks.spark.csv").option("nullValue","null").option("mode", "FAILFAST").load,
        'json': spark.read.json,
    }

    path = args.fin.replace("/hdfs/analytix.cern.ch", "")
    if args.format == 'infer':
        for k in formats.keys():
            if k in path:
                args.format = k
                print("Selecting format %s to open %s" % (k, path))
                break
        if args.format == 'infer':
            raise Exception("Could not infer data source format from path")

    df = formats[args.format](path)
    rows = df.sample(min(1., float(args.samples)/df.count())).collect()

    if args.oformat == 'json':
        with open(args.fout, "w") as fout:
            json.dump([row.asDict(recursive=True) for row in rows], fout)
    elif args.oformat == 'pandas-pickle':
        rows.toPandas().to_pickle(args.fout)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=
            "Randomly sample records from a spark data source on HDFS and output to a file.  Runs using spark-submit."
            "Example paths available at https://github.com/dmwm/CMSSpark/wiki/Data-streams ."
            "If accessing avro, please run prefixed with `spark-submit --packages com.databricks:spark-avro_2.11:4.0.0`"
            )
    parser.add_argument("fin", metavar="INPUT", help="Input data source on hdfs. If leading '/hdfs/analytix.cern.ch' is found, it is removed automatically.")
    parser.add_argument("fout", metavar="OUTPUT", help="Output filename")
    parser.add_argument("-i", "--input-format", dest="format", help="Use the spark reader for format (default: infer from path name)", choices=['avro', 'csv', 'json', 'infer'], default='infer')
    parser.add_argument("-o", "--output-format", dest="oformat", help="Output format (default: json)", choices=['json', 'pandas-pickle'], default='json')
    parser.add_argument("-n", "--samples", dest="samples", help="Approximate number of samples to take from data source (default: 100)", type=int, default=100)

    args = parser.parse_args()
    sample(args)

