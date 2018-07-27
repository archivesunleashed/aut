/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source platform for analyzing web archives.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.archivesunleashed.app

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import io.archivesunleashed._
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val files: ScallopOption[Int] = opt[Int](descr = "number of warc files", required = true)
  verify()
}

class MyPartitioner(numPars: Int) extends Partitioner {
  def numPartitions: Int = numPars
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    (k.hashCode() & Integer.MAX_VALUE) % numPartitions
  }
}


/**
  * Calculate MIME Distribution of a web archive
  */
object MIMEDistribution {

  def getFiles(dir: Path, numFiles: Option[Int], fs: FileSystem): String = {
    val indexFiles = fs.listStatus(dir)
    var arcFiles = indexFiles.filter(f => f.isFile).map(f => f.getPath)
    if (numFiles.isDefined) {
      arcFiles = arcFiles.take(numFiles.get)
    }
    arcFiles.mkString(",")
  }

  val log: Logger = Logger.getLogger(getClass.getName)

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("MIMEDistribution")
    val sc = new SparkContext(conf)

    // remove existing output directory
    val outputDir = new Path(args.output())
    val fs = FileSystem.getLocal(sc.hadoopConfiguration)
    fs.delete(outputDir, true)

    val path = new Path(args.input())
    val indexFiles = fs.listStatus(path)
    //var arcFiles = indexFiles.filter(f => f.isFile && f.getPath.getName.endsWith(".gz")).map(f => f.getPath)
    var arcFiles = indexFiles.filter(f => f.isFile && f.getPath.toString.contains("DOTGOV-EXTRACTION-1995-FY2013-MIME-VIDEO-ARCS-PART-00021")).map(f => f.getPath)
    log.info("Number of ARC files:" + arcFiles.length)


    //val warcCollections = warcFiles.sliding(10, 10).toList.map(c => c.mkString(",")).take(args.files())
    //val warcCollection = warcFiles.toList.take(args.files()).mkString(",")
    val arcCollection = arcFiles.mkString(",")

    // you can specify input as a list of comma separated files or a directory
    //val files = getFiles(path, args.files.toOption, fs) else args.input()
    log.info("Perform analysis on: " + args.input())

//    var i = 0
//    for (warcCollection <- warcCollections) {
//      i += 1
//      RecordLoader.loadArchives(warcCollection, sc)
//        .filter(r => r.getMimeType != null && !r.getMimeType.contains("NullType"))
//        .map(r => (r.getMimeType, 1))
//        .reduceByKey(_ + _)
//        .sortBy(_._2, ascending = false)
//        .saveAsTextFile(args.output() + i)
//    }


    RecordLoader.loadArchives(arcCollection, sc)
      .filter(r => r.getMimeType != null && !r.getMimeType.contains("NullType"))
      .map(r => (r.getMimeType, 1))
      .repartitionAndSortWithinPartitions(new MyPartitioner(88))
      .reduceByKey(_ + _)
//      .sortBy(_._2, ascending = false)
      .saveAsTextFile(args.output())
  }
}
