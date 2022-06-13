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

package io.archivesunleashed.df

import io.archivesunleashed.RecordLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

/** DataFrame wrapper for PySpark implementation. * */
class DataFrameLoader(sc: SparkContext) {

  /** Create a DataFrame with crawl_date, url, mime_type_web_server, mime_type_tika, content, bytes, http_status_code, and archive_filename. */
  def all(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .all()
  }

  /** Create a DataFrame with audio url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def audio(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .audio()
  }

  /* Create a DataFrame with css url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def css(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .css()
  }

  /* Create a DataFrame with html url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def html(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .html()
  }

  /* Create a DataFrame with crawl date, source page, image url, and alt text. */
  def imagegraph(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .imagegraph()
  }

  /** Create a DataFrame with image url, filename, extension, mime_type_web_server, mime_type_tika, width, height, md5, sha1, and raw bytes. */
  def images(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .images()
  }

  /* Create a DataFrame with js url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def js(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .js()
  }

  /* Create a DataFrame with json url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def json(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .json()
  }

  /** Create a DataFrame with PDF url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def pdfs(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .pdfs()
  }

  /* Create a DataFrame with plain-text url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def plainText(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .plainText()
  }

  /** Create a DataFrame with presentation program file url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def presentationProgramFiles(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .presentationProgramFiles()
  }

  /** Create a DataFrame with spreadsheet url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def spreadsheets(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .spreadsheets()
  }

  /** Create a DataFrame with video url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def videos(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .videos()
  }

  /** Create a DataFrame with crawl_date, source, destination, and anchor. */
  def webgraph(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .webgraph()
  }

  /** Create a DataFrame with crawl_date, url, mime_type_web_server, language, and content. */
  def webpages(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .webpages()
  }

  /** Create a DataFrame with word processor file url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and raw bytes. */
  def wordProcessorFiles(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .wordProcessorFiles()
  }

  /* Create a DataFrame with xml url, filename, extension, mime_type_web_server, mime_type_tika, md5, sha1, and content. */
  def xml(path: String): DataFrame = {
    RecordLoader
      .loadArchives(path, sc)
      .xml()
  }
}
