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

package io.archivesunleashed

import com.google.common.io.Resources
// scalastyle:off underscore.import
import io.archivesunleashed.util.TweetUtils._
// scalastyle:on underscore.import
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class RecordLoaderTest extends FunSuite with BeforeAndAfter {
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val tweetPath = Resources.getResource("arc/tweetsTest.json").getPath
  private val delTweetPath = Resources.getResource("arc/delTweetsTest.json").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    conf.set("spark.driver.allowMultipleContexts", "true");
    sc = new SparkContext(conf)
  }

  test("loads Warc") {
    val base = RecordLoader.loadArchives(warcPath, sc)
      .keepValidPages()
      .map(x => x.getUrl)
      .take(1)
    assert(base(0) == "http://www.archive.org/")
  }

  test("loads Tweets") {
    val base = RecordLoader.loadTweets(tweetPath, sc)
      .map(x => x.text())
      .collect()
    assert(base(0) == "some text")
    assert(base(1) == "some more text")
  }

  test("does not load deleted") {
    val base = RecordLoader.loadTweets(delTweetPath, sc).collect()
    assert(base.deep == Array().deep)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
