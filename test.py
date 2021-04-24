from pyspark.sql import SparkSession    
import sys   
spark = SparkSession \
    .builder \
    .appName("Trade Analysis") \
    .config('spark.executor.instances',2) \
    .config('spark.executor.memory','16g') \
    .config('spark.executor.cores', 2) \
    .config('spark.yarn.access.hadoopFileSystems', 's3a://iykra-cdp') \
    .config('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation', 'true') \
    .getOrCreate()