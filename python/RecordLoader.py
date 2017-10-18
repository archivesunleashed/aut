# /*
#  * Archives Unleashed Toolkit (AUT):
#  * An open-source platform for analyzing web archives.
#  *
#  * Licensed under the Apache License, Version 2.0 (the "License");
#  * you may not use this file except in compliance with the License.
#  * You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
from pyspark.rdd import RDD
from pyspark.ml.common import _java2py

from ArcRecord import ArcRecord
from RecordRDD import RecordRDD

# def loadArc(path, sc):
#   inputclass = "io.archivesunleashed.mapreduce.WacArcInputFormat"
#   keyclass = "org.apache.hadoop.io.LongWritable"
#   valueclass = "io.archivesunleashed.io.ArcRecordWritable"
#   return sc.newAPIHadoopFile(path, inputclass, keyclass, valueclass, valueConverter = None) \
#            .map(lambda r: ArcRecord(r[1]))

def loadArcAsRDD(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadArc(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  rdd = pdf.rdd.map(lambda r: rowToArcRecord(r))
  spark.catalog.dropTempView("df")
  return rdd

def loadArcAsDF(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadArc(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  spark.catalog.dropTempView("df")
  return pdf

def loadWarcAsRDD(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadWarc(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  rdd = pdf.rdd.map(lambda r: rowToArcRecord(r))
  spark.catalog.dropTempView("df")
  return rdd

def loadWarcAsDF(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadWarc(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  spark.catalog.dropTempView("df")
  return pdf

def loadArchivesAsDF(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadArchives(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  spark.catalog.dropTempView("df")
  return pdf

def loadArchivesAsRDD(path, sc, spark):
  rlph = sc._jvm.io.archivesunleashed.pyspark.matchbox.RecordLoaderPythonHelper
  df = rlph.loadArchives(path, sc._jsc, spark._jsparkSession)
  df.createTempView("df")
  pdf = spark.table("df")
  #rdd = pdf.rdd.map(lambda r: rowToArcRecord(r))
  spark.catalog.dropTempView("df")
  return pdf.rdd

def rowToArcRecord(row):
  contentBytes = row['contentBytes']
  contentString = row['contentString']
  crawlDate = row['crawlDate']
  crawlMonth = row['crawlMonth']
  domain = row['domain']
  imageBytes = row['imageBytes']
  mimeType = row['mimeType']
  url = row['url'] 
  return ArcRecord(contentBytes, contentString, crawlDate, crawlMonth, 
    domain, imageBytes, mimeType, url)

  
 
 