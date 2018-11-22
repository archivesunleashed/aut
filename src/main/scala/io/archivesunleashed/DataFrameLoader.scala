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

 package io.archivesunleashed

import org.apache.spark.SparkContext
// scalastyle:off underscore.import
import org.apache.spark.sql._
// scalastyle:on underscore.import

class DataFrameLoader(sc: SparkContext) {
  def extractValidPages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractValidPagesDF()
  }

  def extractHyperlinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractHyperlinksDF()
  }

  /* Create a dataframe with (source page, image url) pairs */
  def extractImageLinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractImageLinksDF()
  }

  /** Create a dataframe with (image url, type, width, height, md5, raw bytes) pairs */
  def extractImages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractImageDetailsDF()
  }
}
