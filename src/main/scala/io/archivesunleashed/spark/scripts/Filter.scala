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
package io.archivesunleashed.spark.scripts

import org.apache.spark.SparkContext
import io.archivesunleashed.spark.matchbox.{RemoveHTML, RecordLoader}
import io.archivesunleashed.spark.rdd.RecordRDD._

object Filter {
  def filter(sc: SparkContext) = {
    val r = RecordLoader.loadArc("collections/ARCHIVEIT-227-UOFTORONTO-CANPOLPINT-20090201174320-00056-crawling04.us.archive.org.arc.gz", sc)
      .keepMimeTypes(Set("text/html"))
      .discardDate(null)
      .keepDomains(Set("greenparty.ca"))
      .map(r => (r.getCrawlDate, RemoveHTML(r.getContentString)))
    r.saveAsTextFile("/green")
  }
}

