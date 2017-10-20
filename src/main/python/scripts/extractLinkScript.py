# Archives Unleashed Toolkit (AUT):
# An open-source platform for analyzing web archives.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import RecordLoader
from DFTransformations import *
from ExtractDomain import ExtractDomain
from ExtractLinks import ExtractLinks
from pyspark.sql import SparkSession

if __name__ == "__main__":
	# replace with your own path to archive file
	path = "/Users/Prince/Projects/pyaut/aut/example.arc.gz"

	spark = SparkSession.builder.appName("extractLinks").getOrCreate()
	sc = spark.sparkContext

	rdd = RecordLoader.loadArchivesAsRDD(path, sc, spark)
	rdd1 = rdd.flatMap(lambda r: ExtractLinks(r.url, r.contentString))
	rdd2 = rdd1.map(lambda r: (ExtractDomain(r[0]), ExtractDomain(r[1])))
	rdd3 = rdd2.filter(lambda r: r[0] is not None and r[0]!= "" and r[1] is not None and r[1] != "")
	rdd4 = countItems(rdd3).filter(lambda r: r[1] > 5)

	print(rdd4.take(10))

	spark.stop()
