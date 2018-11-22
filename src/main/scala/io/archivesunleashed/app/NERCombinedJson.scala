/*
 * Archives Unleashed Toolkit (AUT):
 * An open-source toolkit for analyzing web archives.
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

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}

import io.archivesunleashed.matchbox.NERClassifier
import io.archivesunleashed.util.JsonUtils
import org.apache.hadoop.conf.Configuration
// scalastyle:off underscore.import
import org.apache.hadoop.fs._
// scalastyle:on underscore.import
import org.apache.spark.SparkContext

import scala.collection.mutable.MutableList
import scala.util.Random

/** Classifies records using NER and stores results as JSON. */
class NERCombinedJson extends Serializable {

  /** Merges the counts from two lists of tuples.
   *
   * @param keyCount1 the first list of tuples (String, Count)
   * @param keyCount2 the second list of tuples to merge into keyCount1
   * @return combined list of (key, Count) tuples.
   */
  def combineKeyCountLists (keyCount1: List[(String, Int)], keyCount2: List[(String, Int)]): List[(String, Int)] = {
    (keyCount1 ++ keyCount2).groupBy(_._1 ).map {
      case (key, tuples) => (key, tuples.map( _._2).sum)
    }.toList
  }

  /** Combines directory of part-files containing one JSON array per line into a
    * single file containing a single JSON array of arrays.
    *
    * @param srcDir name of directory holding files, also name that will
    *               be given to JSON file
    * @return Unit().
    */
  def partDirToFile(srcDir: String): Unit = {
    val randomSample = 8
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val rnd = new Random
    val srcPath = new Path(srcDir)
    val tmpFile = rnd.alphanumeric.take(randomSample).mkString + ".almostjson"
    val tmpPath = new Path(tmpFile)

    // Merge part-files into single file.
    FileUtil.copyMerge(hdfs, srcPath, hdfs, tmpPath, false, hadoopConfig, "")

    // Read file of JSON arrays, write into single JSON array of arrays.
    val fsInStream = hdfs.open(tmpPath)
    val inFile = new BufferedReader(new InputStreamReader(fsInStream))
    hdfs.delete(srcPath, true)  // Don't need part-files anymore
    val fsOutStream = hdfs.create(srcPath, true) // path was dir of part-files,
                                                 // now is a file of JSON
    val outFile = new BufferedWriter(new OutputStreamWriter(fsOutStream))
    outFile.write("[")
    val line: Option[String] = Option(inFile.readLine())
    line match {
      case Some(line) =>
        outFile.write(line)
      case None =>
    }
    Iterator.continually(inFile.readLine()).takeWhile(Option(_) != None)
      .foreach(s => {outFile.write(", " + s)})
    outFile.write("]")
    outFile.close()

    inFile.close()
    hdfs.delete(tmpPath, false)
  }

  /** Do NER classification on input path, output JSON.
    *
    * @param iNerClassifierFile path of classifier file
    * @param inputFile path of file with tuples (date: String, url: String, content: String)
    *                  from which to extract entities
    * @param outputFile path of output file (e.g., "entities.json")
    * @param sc Spark context object
    */
  def classify(iNerClassifierFile: String, inputFile: String, outputFile: String,
    sc: SparkContext): Unit = {
    val out = sc.textFile(inputFile)
      .mapPartitions(iter => {
        NERClassifier.apply(iNerClassifierFile)
        iter.map(line => {
            val substrs = line.split(",", 3)
            (substrs(0), substrs(1), substrs(2))
          })
          .map(r => {
            val classifiedJson = NERClassifier.classify(r._3)
            val classifiedMap = JsonUtils.fromJson(classifiedJson)
            val classifiedMapCountTuples: Map[String, List[(String, Int)]] = classifiedMap.map {
              case (nerType, entities: List[String @unchecked]) => (nerType, entities.groupBy(identity).mapValues(_.size).toList)
            }
            ((r._1, r._2), classifiedMapCountTuples)
          })
      })
      .reduceByKey( (a, b) => (a ++ b).keySet.map(r => (r, combineKeyCountLists(a(r), b(r)))).toMap)
      .mapPartitions(iter => {
        iter.map(r => {
          val nerRec = new NerRecord(r._1._1, r._1._2)
          r._2.foreach(entityMap => {
            // e.g., entityMap = "PERSON" -> List(("Jack", 1), ("Diane", 3))
            val ec = new EntityCounts(entityMap._1)
            entityMap._2.foreach(e => {
              ec.entities += new Entity(e._1, e._2)
            })
            nerRec.ner += ec
          })
          JsonUtils.toJson(nerRec)
        })
      })
      .saveAsTextFile(outputFile)

    partDirToFile(outputFile)
  }

  /** Create a NER Entity.
   *
   * @constructor create an entity with iEntity and iFreq.
   * @param iEntity
   * @param iFreq
   */
  class Entity(iEntity: String, iFreq: Int) {
    var entity: String = iEntity
    var freq: Int = iFreq
  }

  /** Counts the entities from a NER operation.
   *
   * @constructor create an entity count with iNerType.
   * @param iNerType
   */
  class EntityCounts(iNerType: String) {
    var nerType: String = iNerType
    var entities = MutableList[Entity]()
  }

  /** Creates a NER record from a date and a domain.
   *
   * @constructor create a NER record with recDate and recDomain.
   * @param recData
   * @param recDomain
   */
  class NerRecord(recDate: String, recDomain: String) {
    var date = recDate
    var domain = recDomain
    var ner = MutableList[EntityCounts]()
  }
}
