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

import io.archivesunleashed.df.{ExtractDomainDF, RemovePrefixWWWDF}
import io.archivesunleashed.matchbox.{ExtractDomainRDD, ExtractLinksRDD}
import io.archivesunleashed.{ArchiveRecord, DataFrameLoader, CountableRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DomainGraphExtractor {
  /** Extract domain graph from web archive using RDD.
    *
    * @param records RDD[ArchiveRecord] obtained from RecordLoader
    * @return RDD[(String, String, String), Int],
    *         which is ((CrawlDate, SourceDomain, DestinationDomain), Frequency)
    */
  def apply(records: RDD[ArchiveRecord]): RDD[((String, String, String), Int)] = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, ExtractLinksRDD(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f =>
        (r._1,
          ExtractDomainRDD(f._1).replaceAll("^\\s*www\\.", ""),
          ExtractDomainRDD(f._2).replaceAll("^\\s*www\\.", ""))
        ))
      .filter(r => r._2 != "" && r._3 != "")
      .countItems()
      .filter(r => r._2 > 5)
  }

  /** Extract domain graph from web archive using DataFrame and Spark SQL.
    *
    * @param d DataFrame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (CrawlDate, SrcDomain, DestDomain, count)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    d.groupBy(
        $"crawl_date",
        RemovePrefixWWWDF(ExtractDomainDF($"src")).as("src_domain"),
        RemovePrefixWWWDF(ExtractDomainDF($"dest")).as("dest_domain"))
    .count()
    .filter(!($"dest_domain"===""))
    .filter(!($"src_domain"===""))
    .filter($"count" > 5)
    .orderBy(desc("count"))
  }
}
