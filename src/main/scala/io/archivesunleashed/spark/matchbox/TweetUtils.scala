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
package io.archivesunleashed.spark.matchbox

import org.json4s.JsonAST._

object TweetUtils {
  implicit class JsonTweet(tweet: JValue) {
    implicit lazy val formats = org.json4s.DefaultFormats

    def id(): String = try { (tweet \ "id_str").extract[String] } catch { case e: Exception => null}
    def createdAt(): String = try { (tweet \ "created_at").extract[String] } catch { case e: Exception => null}
    def text(): String = try { (tweet \ "text").extract[String] } catch { case e: Exception => null}
    def lang: String = try { (tweet \ "lang").extract[String] } catch { case e: Exception => null}

    def username(): String = try { (tweet \ "user" \ "screen_name").extract[String] } catch { case e: Exception => null}
    def isVerifiedUser(): Boolean = try { (tweet \ "user" \ "screen_name").extract[String] == "false" } catch { case e: Exception => false}

    def followerCount: Int = try { (tweet \ "user" \ "followers_count").extract[Int] } catch { case e: Exception => 0}
    def friendCount: Int = try { (tweet \ "user" \ "friends_count").extract[Int] } catch { case e: Exception => 0}
  }
}
