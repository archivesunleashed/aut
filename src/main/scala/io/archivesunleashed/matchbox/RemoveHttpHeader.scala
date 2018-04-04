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

/**
  * Created by youngbinkim on 7/9/16.
  */
object RemoveHttpHeader {
  val headerEnd = "\r\n\r\n"
  def apply(content: String): String = {
    try {
      if (content.startsWith("HTTP/"))
        content.substring(content.indexOf(headerEnd) + headerEnd.length)
      else
        content
    } catch {
      case e: Exception => {
        println(e)
        null
      }
    }
  }
}
