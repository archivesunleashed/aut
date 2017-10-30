package io.archivesunleashed.spark.pythonhelpers

import io.archivesunleashed.spark.archive.io.ArchiveRecord
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import io.archivesunleashed.spark.matchbox.RecordLoader

object RecordLoaderPythonHelper {

  def loadArchives(path: String, jssc: JavaSparkContext, spark: SparkSession, keepValidPages: Boolean = true): DataFrame = {
    val sc = jssc.sc
    val rdd = RecordLoader.loadArchives(path, sc, keepValidPages)
    val df = spark.createDataFrame(rdd = rdd, beanClass = classOf[ArchiveRecord])
    df
  }

  def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
    sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
      .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)

}
