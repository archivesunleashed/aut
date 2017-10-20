package io.archivesunleashed.spark.pythonhelpers

import io.archivesunleashed.io.GenericArchiveRecordWritable.ArchiveFormat
import io.archivesunleashed.io.{ArcRecordWritable, GenericArchiveRecordWritable, WarcRecordWritable}
import io.archivesunleashed.mapreduce.{WacArcInputFormat, WacGenericInputFormat, WacWarcInputFormat}
import io.archivesunleashed.spark.archive.io.{ArcRecord, ArchiveRecord, GenericArchiveRecord, WarcRecord}
import org.apache.hadoop.io.LongWritable
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext, JavaPairRDD}
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import io.archivesunleashed.spark.matchbox.RecordLoader
import io.archivesunleashed.spark.rdd.RecordRDD._

object RecordLoaderPythonHelper {
//  def loadArc(path: String, jssc: JavaSparkContext): JavaRDD[ArcRecordWritable] = {
//    val sc = jssc.sc
//    //val rdd = RecordLoader.loadArc(path, sc)
//    val rdd = sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
//      .map(r => r._2)
//    val jrdd = new JavaRDD(rdd)
//    jrdd
//  }

  def loadArc(path: String, jssc: JavaSparkContext, spark: SparkSession): DataFrame = {
    val sc = jssc.sc
    val rdd = RecordLoader.loadArc(path, sc).keepValidPages()
    val df = spark.createDataFrame(rdd = rdd, beanClass = classOf[ArchiveRecord])
    df
  }

  def loadWarc(path: String, jssc: JavaSparkContext, spark: SparkSession): DataFrame = {
    val sc = jssc.sc
    val rdd = RecordLoader.loadWarc(path, sc).keepValidPages()
    val df = spark.createDataFrame(rdd = rdd, beanClass = classOf[ArchiveRecord])
    df
  }

  def loadArchives(path: String, jssc: JavaSparkContext, spark: SparkSession): DataFrame = {
    val sc = jssc.sc
    val rdd = RecordLoader.loadArchives(path, sc).keepValidPages()
    val df = spark.createDataFrame(rdd = rdd, beanClass = classOf[ArchiveRecord])
    df
  }

  def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
    sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
      .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)

  def add(a: Int, b: Int): Int = a + b

  def square(a : List[Int]): List[Int] = a.map(x => x*x)
}
