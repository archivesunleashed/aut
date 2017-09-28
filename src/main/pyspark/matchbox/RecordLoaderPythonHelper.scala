package io.archivesunleashed.pyspark.matchbox

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

object RecordLoaderPythonHelper {
  def loadArc(path: String, jssc: JavaSparkContext): JavaRDD[ArchiveRecord] = {
    val sc = jssc.sc
    val rdd = RecordLoader.loadArc(path, sc)
    val jrdd = new JavaRDD(rdd)
    jrdd
  }

  def loadWarc(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacWarcInputFormat], classOf[LongWritable], classOf[WarcRecordWritable])
      .filter(r => r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response"))
      .map(r => new WarcRecord(new SerializableWritable(r._2)))
  }

  def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacGenericInputFormat], classOf[LongWritable], classOf[GenericArchiveRecordWritable])
      .filter(r => (r._2.getFormat == ArchiveFormat.ARC) ||
        ((r._2.getFormat == ArchiveFormat.WARC) && r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response")))
      .map(r => new GenericArchiveRecord(new SerializableWritable(r._2)))
  }

  def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
    sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
      .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)

  def add(a: Int, b: Int): Int = a + b

  def square(a : List[Int]): List[Int] = a.map(x => x*x)
}
