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
package io.archivesunleashed.matchbox

import java.security.MessageDigest
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.commons.codec.binary.Hex


/** Information about an image. e.g. width, height. */
class ImageDetails(imageUrl: String, imageType: String, bytes: Array[Byte]) {
  val dimensions = ComputeImageSize(bytes);
  val width = dimensions._1
  val height = dimensions._2
  val url: String = imageUrl
  val mimeType: String = imageType
  val hash: String = new String(Hex.encodeHex(MessageDigest.getInstance("MD5").digest(bytes)))
  val body: String = Base64.getEncoder.encodeToString(bytes)
}

/** Extracts image details given raw bytes (using Apache Tika). */
object ExtractImageDetails {

  /**
   * @param bytes the raw bytes of the image
   * @return A tuple containing the width and height of the image
   */
  def apply(url: String, mimeType: String, bytes: Array[Byte]): ImageDetails = {
    new ImageDetails(url, mimeType, bytes)
  }
}
