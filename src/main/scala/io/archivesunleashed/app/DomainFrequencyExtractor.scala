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

import io.archivesunleashed.{ArchiveRecord, DataFrameLoader, CountableRDD}
import io.archivesunleashed.matchbox
import io.archivesunleashed.df
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DomainFrequencyExtractor {
  /** Extract domain frequency from web archive using MapReduce.
    *
    * @param records RDD[ArchiveRecord] obtained from RecordLoader
    * @return RDD[(String,Int))], which holds (DomainName, DomainFrequency)
    */
  def apply(records: RDD[ArchiveRecord]): RDD[(String, Int)] = {
      records
        .keepValidPages()
        .map(r => matchbox.ExtractDomain(r.getUrl))
        .countItems()
  }

  /** Extract domain frequency from web archive using Data Frame and Spark SQL.
    *
    * @param d Data frame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (Domain, count)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._ 
    // scalastyle:on

    d.select(df.ExtractBaseDomain($"Url").as("Domain"))
      .groupBy("Domain").count().orderBy(desc("count"))
  }
}
