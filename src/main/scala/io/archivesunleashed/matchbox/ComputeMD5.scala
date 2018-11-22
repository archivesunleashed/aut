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

/** Compute MD5 checksum. */
// scalastyle:off object.name
object ComputeMD5 {
// scalastyle:on object.name
  /** Computes the MD5 checksum of a byte array (eg. an image).
    *
    * For string data, it is better to use `StringUtils.computeHash()`.
    *
    * @param bytes
    * @return MD5 checksum.
    */
  def apply(bytes: Array[Byte]): String = {
    new String(MessageDigest.getInstance("MD5").digest(bytes))
  }
}
