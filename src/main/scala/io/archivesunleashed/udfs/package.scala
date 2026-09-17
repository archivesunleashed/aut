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

import io.archivesunleashed.matchbox.{
  ComputeImageSize,
  ComputeMD5,
  ComputeSHA1,
  DetectLanguage,
  DetectMimeTypeTika,
  ExtractBoilerpipeText,
  ExtractDate,
  ExtractDomain,
  ExtractImageLinks,
  ExtractLinks,
  GetExtensionMIME,
  RemoveHTML,
  RemoveHTTPHeader
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

/** Package object providing UDFs for DataFrames in Scala and PySpark. * */
package object udfs extends Serializable {

  // Matchbox
  def computeImageSize: UserDefinedFunction =
    udf(ComputeImageSize.apply(_: Array[Byte]))
  def computeMD5: UserDefinedFunction = udf(ComputeMD5.apply(_: Array[Byte]))
  def computeSHA1: UserDefinedFunction = udf(ComputeSHA1.apply(_: Array[Byte]))
  def detectLanguage: UserDefinedFunction = udf(DetectLanguage.apply(_: String))
  def detectMimeTypeTika: UserDefinedFunction =
    udf(DetectMimeTypeTika.apply(_: Array[Byte]))
  def extractBoilerpipeText: UserDefinedFunction =
    udf(ExtractBoilerpipeText.apply(_: String))
  def extractDate: UserDefinedFunction =
    udf(ExtractDate.apply(_: String, _: String))
  // Broadcasts the public suffix list once per Spark application instead of
  // letting the single-argument ExtractDomain.apply(url) trigger an
  // independent network fetch on every executor JVM (see
  // ExtractDomain.broadcast). Rebuilt fresh each time this def is accessed,
  // but ExtractDomain.broadcast itself caches by applicationId, so this
  // stays cheap after the first call.
  def extractDomain: UserDefinedFunction = {
    val suffixes = ExtractDomain.broadcast(SparkSession.builder().getOrCreate().sparkContext)
    udf((url: String) => ExtractDomain(url, suffixes.value))
  }
  def extractImageLinks: UserDefinedFunction =
    udf(ExtractImageLinks.apply(_: String, _: String))
  def extractLinks: UserDefinedFunction =
    udf(ExtractLinks.apply(_: String, _: String))
  def getExtensionMime: UserDefinedFunction =
    udf(GetExtensionMIME.apply(_: String, _: String))
  def removeHTML: UserDefinedFunction = udf(RemoveHTML.apply(_: String))
  def removeHTTPHeader: UserDefinedFunction =
    udf(RemoveHTTPHeader.apply(_: String))
  def removePrefixWWW: UserDefinedFunction =
    udf[String, String](_.replaceAll("^\\s*www\\.", ""))

  // Filters
  def hasContent: UserDefinedFunction =
    udf((c: String, contentREs: Seq[String]) => {
      contentREs
        .map(re =>
          (re.r findFirstIn c) match {
            case Some(v) => true
            case None => false
          })
        .exists(identity)
    })
  def hasDate: UserDefinedFunction =
    udf((date: String, dates: Seq[String]) => {
      dates
        .map(re =>
          date match {
            case re.r() => true
            case _ => false
          })
        .exists(identity)
    })
  def hasDomains: UserDefinedFunction =
    udf((domain: String, domains: Seq[String]) => domains.contains(domain))
  def hasHTTPStatus: UserDefinedFunction =
    udf((statusCode: String, statusCodes: Seq[String]) => statusCodes.contains(statusCode))
  def hasImages: UserDefinedFunction =
    udf((date: String, mimeType: String) => date != null && mimeType.startsWith("image/"))
  def hasLanguages: UserDefinedFunction =
    udf((language: String, languages: Seq[String]) => languages.contains(language))
  def hasMIMETypes: UserDefinedFunction =
    udf((mimeType: String, mimeTypes: Seq[String]) => mimeTypes.contains(mimeType))
  def hasMIMETypesTika: UserDefinedFunction =
    udf((mimeType: String, mimeTypesTika: Seq[String]) => mimeTypesTika.contains(mimeType))
  def hasUrlPatterns: UserDefinedFunction =
    udf((urlPattern: String, urlREs: Seq[String]) => {
      urlREs
        .map(re =>
          urlPattern match {
            case re.r() => true
            case _ => false
          })
        .exists(identity)
    })
  def hasUrls: UserDefinedFunction =
    udf((url: String, urls: Seq[String]) => urls.contains(url))
}
