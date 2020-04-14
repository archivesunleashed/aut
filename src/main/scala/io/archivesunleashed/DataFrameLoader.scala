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

package io.archivesunleashed

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

class DataFrameLoader(sc: SparkContext) {

  /** Create a DataFram with crawl_date, url, mime_type_web_server, and content. */
  def webpages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .webpages()
  }

  /** Create a DataFram with crawl_date, url, mime_type_web_server, content and bytes. */
  def all(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .keepValidPages()
      .all()
  }

  /** Create a DataFrame with crawl_date, source, destination, and anchor. */
  def webgraph(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .webgraph()
  }

  /* Create a DataFrame with crawl date, source page, image url, and alt text. */
  def imagegraph(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .imagegraph()
  }

  /** Create a DataFrame with image url, filename, extension, mime_type_web_servr, mime_type_tika, width, height, md5, and raw bytes. */
  def images(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .images()
  }

  /** Create a DataFrame with PDF url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def pdfs(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .pdfs
  }
  /** Create a DataFrame with audio url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def audio(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .audio
  }

  /** Create a DataFrame with video url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def videos(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .videos
  }

  /** Create a DataFrame with spreadsheet url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def spreadsheets(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .spreadsheets
  }

  /** Create a DataFrame with presentation program url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def presentationProgramFiles(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .presentationProgramFiles
  }

  /** Create a DataFrame with word processor url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def wordProcessorFiles(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .wordProcessorFiles
  }

  /** Create a DataFrame with text file url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def textFiles(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .textFiles
  }
}
