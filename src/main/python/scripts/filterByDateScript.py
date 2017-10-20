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
from ExtractDate import DateComponent
from RemoveHTML import RemoveHTML
from pyspark.sql import SparkSession

if __name__ == "__main__":
	# replace with your own path to archive file
	path = "/Users/Prince/Projects/pyaut/aut/example.arc.gz"
	
	spark = SparkSession.builder.appName("filterByDate").getOrCreate()
	sc = spark.sparkContext

	df = RecordLoader.loadArchivesAsDF(path, sc, spark)
	filtered_df = keepDate(df, "2008", DateComponent.YYYY).filter(df['url'].like("%archive%"))
	rdd = filtered_df.rdd
	rdd.map(lambda r: (r.crawlDate, r.domain, r.url, RemoveHTML(r.contentString))) \
	   .saveAsTextFile("out/")

	spark.stop()
