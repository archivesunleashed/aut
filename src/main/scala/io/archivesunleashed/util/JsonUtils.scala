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
package io.archivesunleashed.util

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** JSON utilities extension to work with Twitter API data (JSON). */
object JsonUtils extends Serializable {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

/** Convert a Scala Map to a JSON string.
  *
  * @param value a Scala Map of keys and values
  * @return a json string.
  */
  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }
  /** Convert a Scala Object or other mappable to a JSON string.
    *
    * @param value any mappable object
    * @return a json string.
    */
  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }
  /** Ingest a JSON string and produce a Map Object.
    *
    * @param value a json string
    * @return a map of [key, value] based on json string.
    */
  def fromJson(json: String): Map[String, Any] = {
    mapper.readValue(json, classOf[Map[String, Any]])
  }
}
