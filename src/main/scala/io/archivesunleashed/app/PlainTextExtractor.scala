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

import io.archivesunleashed.{ArchiveRecord, df}
import io.archivesunleashed.matchbox.RemoveHTML
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PlainTextExtractor {
  /** Extract plain text from web archive using MapReduce.
    *
    * @param records RDD[ArchiveRecord] obtained from RecordLoader
    * @return RDD[(String, String, String, String)], which holds
    *         (CrawlDate, Domain, Url, Text)
    */
  def apply(records: RDD[ArchiveRecord]): RDD[(String, String, String, String)] = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, r.getDomain, r.getUrl, RemoveHTML(r.getContentString)))
  }

  /** Extract plain text from web archive using Data Frame and Spark SQL.
    *
    * @param d Data frame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (CrawlDate, Domain, Url, Text)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    d.select($"CrawlDate", df.ExtractBaseDomain($"Url").as("Domain"),
      $"Url", df.RemoveHTML($"Content").as("Text"))
  }
}
