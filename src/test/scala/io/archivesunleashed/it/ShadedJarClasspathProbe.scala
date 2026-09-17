/*
 * Copyright © 2017 The Archives Unleashed Project
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

package io.archivesunleashed.it

// Forked by ShadedJarClasspathIT with a classpath of [fat jar, an old
// commons-io jar]. Not run by surefire itself -- it has no assertions of
// its own, it just needs to run to completion and print its result so the
// IT test can check the forked process's exit code and output.
object ShadedJarClasspathProbe {
  def main(args: Array[String]): Unit = {
    val xml = "<?xml version=\"1.0\"?><root><child/></root>".getBytes("UTF-8")
    val result = io.archivesunleashed.matchbox.DetectMimeTypeTika(xml)
    // scalastyle:off println
    println("PROBE_RESULT=" + result)
    // scalastyle:on println
  }
}
