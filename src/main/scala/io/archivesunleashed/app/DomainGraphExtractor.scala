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

import io.archivesunleashed._
import io.archivesunleashed.matchbox.{ExtractDomain, ExtractLinks}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object DomainGraphExtractor {
  def apply(records: RDD[ArchiveRecord]) = {
    records
      .keepValidPages()
      .map(r => (r.getCrawlDate, ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).replaceAll("^\\\\s*www\\\\.", ""), ExtractDomain(f._2).replaceAll("^\\\\s*www\\\\.", ""))))
      .filter(r => r._2 != "" && r._3 != "")
      .countItems()
      .filter(r => r._2 > 5)
  }
}
