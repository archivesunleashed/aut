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
package io.archivesunleashed.matchbox

/** Information about an image. e.g. width, height*/
class ImageDetails(w: Int, h: Int) {
  val width: Int = w
  val height: Int = h
}

/** Extracts image details given raw bytes (using Apache Tika) */
object ExtractImageDetails {

  /**
   * @param bytes the raw bytes of the image
   * @return A tuple containing the width and height of the image
  */
  def apply(bytes: Array[Byte]): ImageDetails = {
    val dimensions = ComputeImageSize(bytes);
    val width = dimensions._1
    val height = dimensions._2
    return new ImageDetails(width, height)
  }
}