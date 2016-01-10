__author__ = 'pranavgoel'

from pyspark import SparkConf,SparkContext,SQLContext
import sys
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_range_SQL(df,sqlContext):

    df.registerTempTable("weather")

    # finding the range for the MAX and MIN temperature and filtering data to remove QFLAG values
    range1 = sqlContext.sql("Select tmax.station,tmax.date, (tmax.value - tmin.value) as range \
                            FROM weather tmax , weather tmin where \
                            tmax.qflag = '' AND tmin.qflag = '' AND\
                            tmax.date = tmin.date AND tmax.station = tmin.station AND \
                            tmax.element = 'TMAX' AND tmin.element = 'TMIN' ").cache()

    #range_df = sqlContext.createDataFrame(range1)
    range1.registerTempTable("range_table")
    # Max range for a day
    range_max = sqlContext.sql("select rt.date , MAX(rt.range) as range FROM range_table rt GROUP BY rt.date")
    range_max.registerTempTable("range_max")
    #Range for a day and station corresponding to it with the sorted result
    range_station = sqlContext.sql("select rm.date , rt.station, rm.range from range_table rt , range_max rm \
                                    where rm.date = rt.date AND rm.range = rt.range ORDER By rm.date ASC")

    return(range_station)

def get_output_format(df_Range):

    #converting dataframe to rdd and modifying the format to space-delimited format to save as text file
    rdd_row = df_Range.rdd
    out_Format = rdd_row.map(lambda (date,station,range): "%s %s %i" %(date,station,range)).coaleace(1)
    return (out_Format)


def main():
    inputs = sys.argv[1]
    output = sys.argv[2]
    conf = SparkConf().setAppName('Range')
    sc = SparkContext(conf=conf)
    assert sc.version >= '1.5.1'
    sqlContext = SQLContext(sc)
    #createing cutom schema to load csv files without headers and assigning headers
    customSchema = StructType([
    StructField("station", StringType(), False),
    StructField("date", StringType(), False),
    StructField("element", StringType(), False),
    StructField("value", IntegerType(), True),
    StructField("value1", StringType(), True),
    StructField("qflag", StringType(), False),
    StructField("value2", StringType(), True),
    StructField("value3", StringType(), True)
    ])

    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs, schema = customSchema)

    df_Range = get_range_SQL(df,sqlContext)
    outdata = get_output_format(df_Range)
    outdata.saveAsTextFile(output)

if __name__ == "__main__":
    main()