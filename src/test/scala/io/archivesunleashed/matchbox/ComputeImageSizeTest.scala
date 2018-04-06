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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import java.io.ByteArrayOutputStream
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

@RunWith(classOf[JUnitRunner])
class ComputeImageSizeTest extends FunSuite {
  var ios: ByteArrayOutputStream = new ByteArrayOutputStream();
  val img = new BufferedImage(10, 10, BufferedImage.TYPE_INT_RGB)
  ImageIO.write(img, "png", ios)
  ios.flush()
  var image: Array[Byte] = ios.toByteArray();
  ios.close()

  test ("check images") {
    assert(ComputeImageSize(image) == (10, 10))
    assert(ComputeImageSize(Array[Byte](0,0,0)) == (0, 0))
    assert(ComputeImageSize(null) == (0,0))
  }
}
