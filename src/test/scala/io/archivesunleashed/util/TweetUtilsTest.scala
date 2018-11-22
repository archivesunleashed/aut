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
import io.archivesunleashed.util.TweetUtils._
import org.json4s._
import org.json4s.jackson.JsonMethods._
// scalastyle:on underscore.import
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TweetUtilsTest extends FunSuite {
  test("remove prefix") {
    var tweet:org.json4s.JValue = parse("""{"id_str":"123",
      "created_at":"20150702",
      "text": "some text",
      "full_text": "some full text",
      "lang": "en",
      "retweet_count": 12345,
      "favorite_count": 98765,
      "in_reply_to_status_id_str": "12345",
      "in_reply_to_user_id_str": "5647",
      "in_reply_to_screen_name": "ianmilligan1",
      "source": "<a href='http://twitter.com' rel='nofollow'>Twitter Web Client</a>",
      "user": {
      "screen_name": "twitteruser",
      "verified": true,
      "protected": true,
      "profile_image_url": "https://pbs.twimg.com/profile_images/988576161797042177/xBrHGaCj_400x400.jpg",
      "description": "some description",
      "location": "Detroit, MI",
      "name": "Nick Ruest",
      "url": "https://archivesunleashed.org/",
      "time_zone": "Eastern Time (US & Canada)",
      "followers_count": 45,
      "friends_count": 758}}""")
    val expectedFollowers = 45
    val expectedFriends = 758
    val expectedRetweetCount = 12345
    val expectedFavoriteCount = 98765
    assert(tweet.id() == "123")
    assert(tweet.createdAt() == "20150702")
    assert(tweet.text() == "some text")
    assert(tweet.fullText() == "some full text")
    assert(tweet.retweetCount == expectedRetweetCount)
    assert(tweet.favoriteCount == expectedFavoriteCount)
    assert(tweet.inReplyToStatusId() == "12345")
    assert(tweet.inReplyToUserId() == "5647")
    assert(tweet.inReplyToScreenName() == "ianmilligan1")
    assert(tweet.source() == "<a href='http://twitter.com' rel='nofollow'>Twitter Web Client</a>")
    assert(tweet.isUserProtected())
    assert(tweet.profileImageUrl() == "https://pbs.twimg.com/profile_images/988576161797042177/xBrHGaCj_400x400.jpg")
    assert(tweet.profileDescription() == "some description")
    assert(tweet.profileLocation() == "Detroit, MI")
    assert(tweet.profileName() == "Nick Ruest")
    assert(tweet.profileUrl() == "https://archivesunleashed.org/")
    assert(tweet.profileTimezone() == "Eastern Time (US & Canada)")
    assert(tweet.lang == "en")
    assert(tweet.username() == "twitteruser")
    assert(tweet.isVerifiedUser())
    assert(tweet.followerCount == expectedFollowers)
    assert(tweet.friendCount == expectedFriends)
  }

  test("errors") {
    var tweet:org.json4s.JValue = parse("""{"id_str": null,
      "created_at":null,
      "text": null,
      "full_text": null,
      "lang": null,
      "retweet_count": null,
      "favorite_count": null,
      "in_reply_to_status_id_str": null,
      "in_reply_to_user_id_str": null,
      "in_reply_to_screen_name": null,
      "source": null,
      "user": {
      "screen_name": null,
      "verified" : null,
      "protected": false,
      "profile_image_url": null,
      "description": null,
      "location": null,
      "name": null,
      "url": null,
      "time_zone": null,
      "followers_count": null,
      "friends_count": null}}""")
    val expected = 0

      assert(tweet.id() == null)
      assert(tweet.createdAt() == null)
      assert(tweet.text() == null)
      assert(tweet.fullText() == null)
      assert(tweet.lang == null)
      assert(tweet.username() == null)
      assert(!tweet.isVerifiedUser())
      assert(tweet.followerCount == expected)
      assert(tweet.friendCount == expected)
      assert(tweet.retweetCount == expected)
      assert(tweet.favoriteCount == expected)
      assert(tweet.inReplyToScreenName() == null)
      assert(tweet.inReplyToStatusId() == null)
      assert(tweet.inReplyToUserId() == null)
      assert(tweet.source() == null)
      assert(tweet.isUserProtected() == false)
      assert(tweet.profileImageUrl() == null)
      assert(tweet.profileDescription() == null)
      assert(tweet.profileLocation() == null)
      assert(tweet.profileName() == null)
      assert(tweet.profileUrl() == null)
      assert(tweet.profileTimezone() == null)
  }
}
