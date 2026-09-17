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

package io.archivesunleashed

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.io.File
import scala.sys.process._

/**
 * Guards against a real regression: shipping the fat jar with an un-relocated dependency (e.g.
 * commons-io) that collides with an older copy of the same library bundled by the Spark/Hadoop
 * distribution a user actually runs it against (e.g. spark-3.3.1-bin-hadoop2, which bundles
 * Hadoop 2.x's commons-io 2.4). `mvn test` alone can't catch this: it runs against Maven's
 * resolved compile classpath, never against the actual shaded/relocated fat jar. This forks a JVM
 * with the fat jar plus a deliberately old commons-io on its classpath -- the same combination
 * that broke DetectMimeTypeTika (a NoSuchMethodError on CloseShieldInputStream.wrap, added in
 * commons-io 2.7) against a real spark-shell run -- and checks it still works.
 */
@RunWith(classOf[JUnitRunner])
class ShadedJarClasspathIT extends FunSuite {
  test("DetectMimeTypeTika works when the fat jar runs alongside an old commons-io") {
    val targetDir = new File("target")
    val fatJar = Option(targetDir.listFiles())
      .flatMap(_.find(f => f.getName.matches("aut-.*-fatjar\\.jar")))
    assert(
      fatJar.isDefined,
      "fat jar not found in target/ -- this test requires `mvn verify` " +
        "(or `package` before `mvn failsafe:integration-test`), not `mvn test` alone")

    val oldCommonsIo =
      new File("target/it-classpath/commons-io-old.jar")
    assert(
      oldCommonsIo.exists(),
      s"${oldCommonsIo.getPath} not found -- expected the " +
        "maven-dependency-plugin copy-poison-commons-io execution to have run first")

    // The old jar must come first: this mirrors a real spark-submit/
    // spark-shell run, where the Spark distribution's own system
    // classpath (which is where the old commons-io actually comes from,
    // via Hadoop) is searched before a --jars-supplied fat jar. Listing
    // the fat jar first here would let its own (correct) copy of the
    // class win before the JVM ever reaches the old one, which is
    // exactly backwards from what happens in practice and would make
    // this test unable to catch the regression it exists to catch.
    val classpath = Seq(oldCommonsIo.getPath, fatJar.get.getPath, "target/test-classes")
      .mkString(File.pathSeparator)

    val cmd = Seq("java", "-cp", classpath, "io.archivesunleashed.it.ShadedJarClasspathProbe")
    val output = new StringBuilder
    val exitCode = cmd ! ProcessLogger(line => output.append(line).append("\n"))

    assert(exitCode == 0, s"probe process failed (exit $exitCode):\n$output")
    assert(
      !output.toString.contains("NoSuchMethodError"),
      s"an un-relocated dependency in the fat jar collided with the old " +
        s"commons-io on the classpath:\n$output")
    assert(
      output.toString.contains("PROBE_RESULT="),
      s"probe did not print the expected result:\n$output")
  }
}
