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

/** Trait for a record in a web archive. */
trait ArchiveRecord extends Serializable {

  /** Returns the full path or url containing the Archive Records. */
  def getArchiveFilename: String

  /** Returns the crawl date. */
  def getCrawlDate: String

  /** Returns the crawl month. */
  def getCrawlMonth: String

  /** Returns the content of the record as a String. */
  def getContentString: String

  /** Returns the MIME type. */
  def getMimeType: String

  /** Returns the URL. */
  def getUrl: String

  /** Returns the domain. */
  def getDomain: String

  /** Returns a raw array of bytes for an image. */
  def getBinaryBytes: Array[Byte]

  /** Returns the http status of the crawl. */
  def getHttpStatus: String

  /** Returns payload digest (SHA1). */
  def getPayloadDigest: String
}
