import RecordLoader
from Transformations import *
from ExtractDomain import ExtractDomain
from ExtractLinks import ExtractLinks
from pyspark.sql import SparkSession

if __name__ == "__main__":
	path = "example.arc.gz"
	spark = SparkSession.builder.appName("extractLinks").getOrCreate()
	sc = spark.sparkContext

	rdd = RecordLoader.loadArchivesAsRDD(path, sc, spark)
	rdd1 = rdd.flatMap(lambda r: ExtractLinks(r.url, r.contentString))
	rdd2 = rdd1.map(lambda r: (ExtractDomain(r[0]), ExtractDomain(r[1])))
	rdd3 = rdd2.filter(lambda r: r[0] is not None and r[0]!= "" and r[1] is not None and r[1] != "")
	rdd4 = countItems(rdd3).filter(lambda r: r[1] > 5)

	print(rdd3.count())

	spark.stop()
