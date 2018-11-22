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

// scalastyle:off underscore.import
import org.json4s.JsonAST._
// scalastyle:on underscore.import

/** Utilities for working with Twitter JSON. */
object TweetUtils {

  /** Extract Twitter API data in json format.
    *
    * @param tweet JValue/JSON object containing Twitter API data (JSON)
    */
  implicit class JsonTweet(tweet: JValue) {
    val user = "user"
    implicit lazy val formats = org.json4s.DefaultFormats
    /** Get tweet id. */
    def id(): String = try { (tweet \ "id_str").extract[String] } catch { case e: Exception => ""}
    /** Get the date a tweet was created. */
    def createdAt(): String = try { (tweet \ "created_at").extract[String] } catch { case e: Exception => ""}
    /** Get the tweet text. */
    def text(): String = try { (tweet \ "text").extract[String] } catch { case e: Exception => ""}
    /** Get the tweet full_text. */
    def fullText(): String = try { (tweet \ "full_text").extract[String] } catch { case e: Exception => ""}
    /** Get the tweet language code (ISO 639-1). */
    def lang: String = try { (tweet \ "lang").extract[String] } catch { case e: Exception => ""}
    /** Get the username of the user who wrote the tweet. */
    def username(): String = try { (tweet \ user \ "screen_name").extract[String] } catch { case e: Exception => ""}
    /** Check if user of tweet is "verified" (true or false). */
    def isVerifiedUser(): Boolean = try { (tweet \ user \ "verified").extract[Boolean] } catch { case e: Exception => false}
    /** Get the number of followers the user has at the time of tweet. */
    def followerCount: Int = try { (tweet \ user \ "followers_count").extract[Int] } catch { case e: Exception => 0}
    /** Get the number of friends (people the person follows) of the user at the time of the tweet. */
    def friendCount: Int = try { (tweet \ user \ "friends_count").extract[Int] } catch { case e: Exception => 0}
    /** Get retweet count of a tweet. */
    def retweetCount: Int = try { (tweet \ "retweet_count").extract[Int] } catch { case e: Exception => 0}
    /** Get favorite count of a tweet. */
    def favoriteCount: Int = try { (tweet \ "favorite_count").extract[Int] } catch { case e: Exception => 0}
    /** Get the tweet id of a tweet that that tweet was in reply to. */
    def inReplyToStatusId(): String = try { (tweet \ "in_reply_to_status_id_str").extract[String] } catch { case e: Exception => ""}
    /** Get the id of a user that that tweet was in reply to. */
    def inReplyToUserId(): String = try { (tweet \ "in_reply_to_user_id_str").extract[String] } catch { case e: Exception => ""}
    /** Get the screen name of a user that that tweet was in reply to. */
    def inReplyToScreenName(): String = try { (tweet \ "in_reply_to_screen_name").extract[String] } catch { case e: Exception => ""}
    /** Get source (application used) of a tweet. */
    def source(): String = try { (tweet \ "source").extract[String] } catch { case e: Exception => ""}
    /** Check is a user's tweets are protected (true or false). */
    def isUserProtected(): Boolean = try { (tweet \ user \ "protected").extract[Boolean] } catch { case e: Exception => false}
    /** Get the profile image url of a user. */
    def profileImageUrl(): String = try { (tweet \ user \ "profile_image_url").extract[String] } catch { case e: Exception => ""}
    /** Get the user's profile description. */
    def profileDescription(): String = try { (tweet \ user \ "description").extract[String] } catch { case e: Exception => ""}
    /** Get the user's provided location. */
    def profileLocation(): String = try { (tweet \ user \ "location").extract[String] } catch { case e: Exception => ""}
    /** Get the user's provided name. */
    def profileName(): String = try { (tweet \ user \ "name").extract[String] } catch { case e: Exception => ""}
    /** Get the user's provided url. */
    def profileUrl(): String = try { (tweet \ user \ "url").extract[String] } catch { case e: Exception => ""}
    /** Get the user's provided timezone. */
    def profileTimezone(): String = try { (tweet \ user \ "time_zone").extract[String] } catch { case e: Exception => ""}
  }
}
