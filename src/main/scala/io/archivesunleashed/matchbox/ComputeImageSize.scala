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

import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

/** Image sizing utilities. */
object ComputeImageSize {

  /** Computes image size from a byte array using ImageIO.
   *
   * Used by `ExtractPopularImages` to calculate the size of
   * the image as a tuple of integers (width, height).
   *
   * @param bytes image as a byte array
   * @return size of image as a tuple (width, height) or (0,0).
   */
  def apply(bytes: Array[Byte]): (Int, Int) = {
    val nullImage = (0, 0)
    try {
      val in = new ByteArrayInputStream(bytes)
      val image = ImageIO.read(in)
      // scalastyle:off null
      if (image == null) {
        nullImage
      }
      // scalastyle:on null
      (image.getWidth(), image.getHeight())
    } catch {
      case e: Throwable => {
        nullImage
      }
    }
  }
}
