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

import io.archivesunleashed.matchbox.{RemoveHTMLRDD, RemoveHTTPHeaderRDD}
import io.archivesunleashed.ArchiveRecord
import io.archivesunleashed.df.{ExtractDomainDF, RemoveHTMLDF,
                                RemoveHTTPHeaderDF}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object PlainTextExtractor {
  /** Extract plain text from web archive using RDD.
    *
    * @param records RDD[ArchiveRecord] obtained from RecordLoader
    * @return RDD[(String, String, String, String)], which is
    *         (crawl date, domain, url, text)
    */
  def apply(records: RDD[ArchiveRecord]): RDD[(String, String, String, String)] = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, r.getDomain, r.getUrl,
        RemoveHTMLRDD(RemoveHTTPHeaderRDD(r.getContentString))))
  }

  /** Extract plain text from web archive using DataFrame and Spark SQL.
    *
    * @param d DataFrame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (crawl date, domain, url, text)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    d.select($"crawl_date", ExtractDomainDF($"url").as("domain"),
      $"url", RemoveHTMLDF(RemoveHTTPHeaderDF($"content")).as("text"))
  }
}
