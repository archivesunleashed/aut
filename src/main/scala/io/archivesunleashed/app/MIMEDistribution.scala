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
  val format: ScallopOption[String] = opt[String](descr = "format of archive files")
  val files: ScallopOption[Int] = opt[Int](descr = "number of warc files")
  val prefix: ScallopOption[String] = opt[String](descr = "prefix")
  val exclude: ScallopOption[String] = opt[String](descr = "exclude")
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

    // you can specify input as a list of comma separated files or a directory
    log.info("Perform analysis on: " + args.input())

    RecordLoader.loadArchive(path.toString, sc, args.format.toOption, args.prefix.toOption, args.files.toOption, args.exclude.toOption)
      .filter(r => r.getMimeType != null && !r.getMimeType.contains("NullType"))
      .map(r => (r.getMimeType, 1))
      .repartitionAndSortWithinPartitions(new MyPartitioner(88))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .saveAsTextFile(args.output())
  }
}
