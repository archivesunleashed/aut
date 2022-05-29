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

import io.archivesunleashed.ArchiveRecord
import io.archivesunleashed.udfs.{extractBoilerpipeText}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.lower

object PlainTextExtractor {

  /** Extract plain text from web archive using DataFrame and Spark SQL.
    *
    * @param d DataFrame obtained from RecordLoader
    * @return Dataset[Row], where the schema is (content)
    */
  def apply(d: DataFrame): Dataset[Row] = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    // scalastyle:off
    import spark.implicits._
    // scalastyle:on
    d.filter($"crawl_date" isNotNull)
      .filter(
        !($"url".rlike(".*robots\\.txt$")) &&
          ($"mime_type_web_server".rlike("text/html") ||
            $"mime_type_web_server".rlike("application/xhtml+xml") ||
            $"url".rlike("(?i).*htm$") ||
            $"url".rlike("(?i).*html$"))
      )
      .filter($"http_status_code" === 200)
      .filter(
        !(lower($"url").startsWith("filedesc:"))
          && (!(lower($"url").startsWith("dns:")))
      )
      .select(extractBoilerpipeText($"raw_content").as("content"))
  }
}
