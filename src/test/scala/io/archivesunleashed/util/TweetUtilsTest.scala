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
package io.archivesunleashed.util

import io.archivesunleashed.util.TweetUtils._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TweetUtilsTest extends FunSuite {
  test("remove prefix") {
    var tweet:org.json4s.JValue = parse("""{"id_str":"123",
      "created_at":"20150702",
      "text": "some text",
      "lang": "en",
      "user": {
      "screen_name": "twitteruser",
      "verified": true,
      "followers_count": 45,
      "friends_count": 758}}""")

    assert(tweet.id() == "123")
    assert(tweet.createdAt() == "20150702")
    assert(tweet.text() == "some text")
    assert(tweet.lang == "en")
    assert(tweet.username() == "twitteruser")
    assert(tweet.isVerifiedUser() == true)
    assert(tweet.followerCount == 45)
    assert(tweet.friendCount == 758)
  }

  test("errors") {
    var tweet:org.json4s.JValue = parse("""{"id_str": null,
      "created_at":null,
      "text": null,
      "lang": null,
      "user": {
      "screen_name": null,
      "verified" : null,
      "followers_count": null,
      "friends_count": null}}""")

      assert(tweet.id() == null)
      assert(tweet.createdAt() == null)
      assert(tweet.text() == null)
      assert(tweet.lang == null)
      assert(tweet.username() == null)
      assert(tweet.isVerifiedUser() == false)
      assert(tweet.followerCount == 0)
      assert(tweet.friendCount == 0)
  }
}
