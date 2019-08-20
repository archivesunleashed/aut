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
import org.apache.spark.sql.DataFrame

class DataFrameLoader(sc: SparkContext) {

  /** Create a DataFram with crawl_date, url, mime_type_web_server, and content. */
  def extractValidPages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractValidPagesDF()
  }

  /** Create a DataFrame with crawl_date, source, destination, and anchor. */
  def extractHyperlinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractHyperlinksDF()
  }

  /* Create a DataFrame with source page, and image url. */
  def extractImageLinks(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractImageLinksDF()
  }

  /** Create a DataFrame with image url, filename, extension, mime_type_web_servr, mime_type_tika, width, height, md5, and raw bytes. */
  def extractImages(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
      .extractImageDetailsDF()
  }

  /** Create a DataFrame with PDF url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractPDFs(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractPDFDetailsDF
  }
  /** Create a DataFrame with audio url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractAudio(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractAudioDetailsDF
  }

  /** Create a DataFrame with video url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractVideo(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractVideoDetailsDF
  }

  /** Create a DataFrame with spreadsheet url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractSpreadsheets(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractSpreadsheetDetailsDF
  }

  /** Create a DataFrame with presentation program url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractPresentationProgram(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractPresentationProgramDetailsDF
  }

  /** Create a DataFrame with word processor url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractWordProcessor(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractWordProcessorDetailsDF
  }

  /** Create a DataFrame with text file url, filename, extension, mime_type_web_servr, mime_type_tika, md5, and raw bytes. */
  def extractTextFiles(path: String): DataFrame = {
    RecordLoader.loadArchives(path, sc)
    .extractTextFilesDetailsDF
  }
}
