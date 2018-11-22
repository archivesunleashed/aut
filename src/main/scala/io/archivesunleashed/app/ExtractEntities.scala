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

// scalastyle:off underscore.import
import io.archivesunleashed._
// scalastyle:on underscore.import
import io.archivesunleashed.matchbox.{NERClassifier, RemoveHTML}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

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
  def extractFromRecords(iNerClassifierFile: String, inputRecordFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = RecordLoader.loadArchives(inputRecordFile, sc)
      .map(r => (r.getCrawlDate, r.getUrl, RemoveHTML(r.getContentString)))
    extractAndOutput(iNerClassifierFile, rdd, outputFile)
  }

  /** Extracts named entities from tuple-formatted derivatives scraped from a website.
    *
    * @param iNerClassifierFile path of classifier file
    * @param inputFile path of file containing tuples (date: String, url: String, content: String)
    *                  from which to extract entities
    * @param outputFile path of output directory
    * @return an rdd with classification entities.
    */
  def extractFromScrapeText(iNerClassifierFile: String, inputFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = sc.textFile(inputFile)
      .map(line => {
        val ind1 = line.indexOf(",")
        val ind2 = line.indexOf(",", ind1 + 1)
        (line.substring(1, ind1),
          line.substring(ind1 + 1, ind2),
          line.substring(ind2 + 1, line.length - 1))
      })
    extractAndOutput(iNerClassifierFile, rdd, outputFile)
  }

  /** Saves the NER output to file from a given RDD.
    *
    * @param iNerClassifierFile path of classifier file
    * @param rdd with values (date, url, content)
    * @param outputFile path of output directory
    * @return an rdd of tuples with classification entities extracted.
    */
  def extractAndOutput(iNerClassifierFile: String, rdd: RDD[(String, String, String)], outputFile: String): RDD[(String, String, String)] = {
    val r = rdd.mapPartitions(iter => {
      NERClassifier.apply(iNerClassifierFile)
      iter.map(r => (r._1, r._2, NERClassifier.classify(r._3)))
    })
    r.saveAsTextFile(outputFile)
    r
  }
}
