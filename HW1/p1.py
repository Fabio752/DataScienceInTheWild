import sys
from pyspark.sql import SparkSession
import argparse

# feel free to def new functions if you need
from pyspark.sql.functions import udf, count, when

map_to_brfss_age_udf = udf(
    lambda nhis_age:
    nhis_age if nhis_age is None else (
        1 if int(nhis_age) < 25 else min(13, int((int(nhis_age) - 15) / 5)))
)

mracbpi2_to_imprace_map = {1: 1, 2: 2, 3: 4, 6: 3, 7: 3, 12: 3, 16: 6, 17: 6}

map_to_brfss_race_udf = udf(
    lambda hispan_i, mracbpi2:
        mracbpi2 if mracbpi2 is None else
    (5 if (hispan_i is not None and int(hispan_i) != 12)
     else mracbpi2_to_imprace_map[int(mracbpi2)])
)


def create_dataframe(filepath, format, spark):
    """
    Create a spark df given a filepath and format.

    :param filepath: <str>, the filepath
    :param format: <str>, the file format (e.g. "csv" or "json")
    :param spark: <str> the spark session

    :return: the spark df uploaded
    """
    # Add your code here
    base_path = "data/p1/"
    if (format == "csv"):
        spark_df = spark.read.option("header", True).csv(base_path + filepath)
    elif (format == "json"):
        spark_df = spark.read.json(base_path + filepath)
    return spark_df


def transform_nhis_data(nhis_df):
    """
    Transform df elements

    :param nhis_df: spark df
    :return: spark df, transformed df
    """

    # add your code here
    transformed_df = nhis_df \
        .withColumn("_AGEG5YR", map_to_brfss_age_udf(nhis_df["AGE_P"])) \
        .withColumn("_IMPRACE", map_to_brfss_race_udf(nhis_df["HISPAN_I"], nhis_df["MRACBPI2"]))

    return transformed_df["SEX", "_IMPRACE", "_AGEG5YR", "DIBEV1"]


def calculate_statistics(joined_df):
    """
    Calculate prevalence statistics

    :param joined_df: the joined df

    :return: None
    """

    # add your code here
    race_joined_df = joined_df.groupBy("_IMPRACE").agg(
        (count(when(joined_df.DIBEV1 == 1, True)) /
         count('DIBEV1')).alias('percentage')
    )

    sex_joined_df = joined_df.groupBy('SEX').agg(
        (count(when(joined_df.DIBEV1 == 1, True)) /
         count('DIBEV1')).alias('percentage')
    )

    age_range_joined_df = joined_df.groupBy("_AGEG5YR").agg(
        (count(when(joined_df.DIBEV1 == 1, True)) /
         count('DIBEV1')).alias('percentage')
    )

    race_joined_df.write.mode("overwrite").csv("./race_stats")
    sex_joined_df.write.mode("overwrite").csv("./sex_stats")
    age_range_joined_df.write.mode("overwrite").csv("./age_range_stats")


def join_data(brfss_df, nhis_df):
    """
    Join dataframes

    :param brfss_df: spark df
    :param nhis_df: spark df after transformation
    :return: the joined df

    """
    # add your code here

    joined_df = brfss_df.na.drop().join(
        nhis_df.na.drop(), ['SEX', "_AGEG5YR", "_IMPRACE"])
    return joined_df


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    arg_parser.add_argument(
        'nhis', type=str, default=None, help="brfss filename")
    arg_parser.add_argument(
        'brfss', type=str, default=None, help="nhis filename")
    arg_parser.add_argument('-o', '--output', type=str,
                            default=None, help="output path(optional)")

    # parse args
    args = arg_parser.parse_args()
    if not args.nhis or not args.brfss:
        arg_parser.usage = arg_parser.format_help()
        arg_parser.print_usage()
    else:
        brfss_filename = args.nhis
        nhis_filename = args.brfss

        # Start spark session
        spark = SparkSession.builder.getOrCreate()

        # load dataframes
        brfss_df = create_dataframe(brfss_filename, 'json', spark)
        nhis_df = create_dataframe(nhis_filename, 'csv', spark)

        # Perform mapping on nhis dataframe
        nhis_df = transform_nhis_data(nhis_df)

        # Join brfss and nhis df
        joined_df = join_data(brfss_df, nhis_df)

        # Calculate statistics
        calculate_statistics(joined_df)

        # Save
        if args.output:
            joined_df.write.csv(args.output, mode='overwrite', header=True)

        # Stop spark session
        spark.stop()
