/*
 * Copyright © 2017 The Archives Unleashed Project
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

import io.archivesunleashed.matchbox.{ComputeMD5RDD, NERClassifier, RemoveHTMLRDD}
import io.archivesunleashed.RecordLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/** Performs Named Entity Recognition (NER) on a WARC or ARC file.
  *
  * [[http://nlp.stanford.edu/software/CRF-NER.html Named Entity Recognition]]
  * applies rules formed in a [[https://stanfordnlp.github.io/CoreNLP  Named
  * Entity Classifier]] to identify locations, people or other objects from data.
  */
object ExtractEntities {

  /** Extracts named entities from WARC or ARC files at a given path to a given output directory.
    *
    * @param iNerClassifierFile path to NER classifier file
    * @param inputRecordFile path of ARC or WARC file from which to extract entities
    * @param outputFile path of output directory
    * @param sc the Apache Spark context
    * @return an rdd with classification entities.
    */
  def extractFromRecords(iNerClassifierFile: String, inputRecordFile: String,
    outputFile: String,
    sc: SparkContext): RDD[(String, String, String, String)] = {
    val rdd = RecordLoader.loadArchives(inputRecordFile, sc)
      .keepValidPages()
      .map(r => (("\"timestamp\":\"" + r.getCrawlDate + "\""),
        ("\"url\":\"" + r.getUrl + "\""),
        (RemoveHTMLRDD(r.getContentString)),
        ("\"digest\":\"" + r.getPayloadDigest + "\"")))
    extractAndOutput(iNerClassifierFile, rdd, outputFile)
  }

  /** Converts output of NER classifier to WANE format
    *
    * @param output of NER Classifier
    * @return output of NER Classifier in WANE format
    */
  def waneFormat(input: String): String = {
    var output = input.replaceAll("PERSON\":","persons\":")
    output = output.replaceAll("ORGANIZATION\":","organizations\":")
    output = output.replaceAll("LOCATION\":","locations\":")
    return output;
  }

  /** Saves the NER output to file from a given RDD.
    *
    * @param iNerClassifierFile path of classifier file
    * @param rdd with values (date, url, content, content digest)
    * @param outputFile path of output directory
    * @return a json object with classification entities extracted.
    */
  def extractAndOutput(iNerClassifierFile: String,
    rdd: RDD[(String, String, String, String)],
    outputFile: String): RDD[(String, String, String, String)] = {
    val r = rdd.mapPartitions(iter => {
      NERClassifier.apply(iNerClassifierFile)
      iter.map(r => (("{" + r._1), r._2,
        ("\"named_entities\":" + waneFormat(NERClassifier.classify(r._3).toString)), (r._4 + "}")))
    })
    r.map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4)
      .saveAsTextFile(outputFile)
    r
  }
}
