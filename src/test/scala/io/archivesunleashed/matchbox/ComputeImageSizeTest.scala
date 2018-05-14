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

import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ComputeImageSizeTest extends FunSuite {
  val width: Int = 10
  val height: Int = 10
  var ios: ByteArrayOutputStream = new ByteArrayOutputStream();
  val img = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
  ImageIO.write(img, "png", ios)
  ios.flush()
  var image: Array[Byte] = ios.toByteArray();
  ios.close()

  test ("check images") {
    assert(ComputeImageSize(image) == (width, height))
    assert(ComputeImageSize(Array[Byte](0,0,0)) == (0, 0))
    assert(ComputeImageSize(null) == (0,0)) // scalastyle:off NullChecker
  }
}
