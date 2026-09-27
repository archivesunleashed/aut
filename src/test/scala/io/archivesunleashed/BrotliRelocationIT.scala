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
import java.util.zip.{ZipEntry, ZipFile}
import scala.collection.JavaConverters._

/**
 * Guards against a real regression: Sparkling's HttpMessage decodes Content-Encoding: br bodies
 * via commons-compress's BrotliCompressorInputStream, which needs org.brotli.dec.BrotliInputStream
 * (a separate dependency AUT declares -- see the org.brotli:dec entry in pom.xml). A real Spark
 * distribution bundles its own, older commons-compress that lacks that decoder (e.g.
 * spark-3.5.9-bin-hadoop3/jars/commons-compress-1.23.0.jar). Since the JVM resolves an unqualified
 * class name via the parent (system) class loader first, it's Spark's own copy of
 * BrotliCompressorInputStream that actually runs against a real spark-shell/spark-submit job, not
 * this fat jar's -- and it fails with NoClassDefFoundError, a fatal LinkageError, failing whatever
 * Spark job hit a Brotli-encoded record. (This is exactly what happened running images() against a
 * real crawl.) The fix is relocating org.apache.commons.compress. in the shade-plugin config, the
 * same way commons-io and Guava already are, for the same reason: so Sparkling's bundled classes
 * call a package-renamed BrotliCompressorInputStream that exists only in this fat jar, right
 * alongside the decoder, leaving no shared name for Spark's own copy to shadow.
 *
 * `mvn test` alone can't catch a regression here, for a different reason than
 * ShadedJarClasspathIT's neighbouring commons-io check: there's no un-relocated dependency to run
 * a probe against, since AUT's own code never calls commons-compress directly, only Sparkling does,
 * transitively. Reconstructing a real HttpMessage with a Content-Encoding: br body just to exercise
 * this one decode path would mostly test Sparkling's own API, not AUT's build. Instead, this checks
 * the actual mechanism the fix relies on: that Sparkling's compiled http package classes, once
 * merged into the fat jar, were rewritten by the shade plugin to reference the relocated
 * BrotliCompressorInputStream rather than the original, un-relocated one.
 */
@RunWith(classOf[JUnitRunner])
class BrotliRelocationIT extends FunSuite {
  private val relocatedRef =
    "org/apache/commons/compress/shaded/compressors/brotli/BrotliCompressorInputStream"
  private val originalRef =
    "org/apache/commons/compress/compressors/brotli/BrotliCompressorInputStream"

  test("Sparkling's HttpMessage.class references the relocated BrotliCompressorInputStream") {
    val targetDir = new File("target")
    val fatJar = Option(targetDir.listFiles())
      .flatMap(_.find(f => f.getName.matches("aut-.*-fatjar\\.jar")))
    assert(
      fatJar.isDefined,
      "fat jar not found in target/ -- this test requires `mvn verify` " +
        "(or `package` before `mvn failsafe:integration-test`), not `mvn test` alone")

    val zip = new ZipFile(fatJar.get)
    val text =
      try {
        // Scanning every class file under Sparkling's http package, rather than guessing which
        // one, sidesteps needing to know exactly where Scala's compiler puts this reference --
        // e.g. a val/def body inside a companion `object` compiles into a separate, `$`-suffixed
        // class file (HttpMessage$.class) from the instance class (HttpMessage.class), and which
        // one actually holds a given reference isn't something to rely on staying the same.
        val entries = zip
          .entries()
          .asScala
          .filter((e: ZipEntry) =>
            e.getName.startsWith("org/archive/webservices/sparkling/http/") &&
              e.getName.endsWith(".class"))
          .toList
        assert(
          entries.nonEmpty,
          "no class files found under org/archive/webservices/sparkling/http/ in the fat jar -- " +
            "has Sparkling's package moved?")
        // Class-file constant-pool entries store class names as plain ASCII (with slashes, not
        // dots). Latin-1 maps every byte value to one character with no decode errors, so this
        // round-trips an ASCII search pattern exactly -- a simple, reliable way to look for a
        // byte sequence in binary data without a real class-file parser.
        entries
          .map { entry =>
            val in = zip.getInputStream(entry)
            try new String(in.readAllBytes(), "ISO-8859-1")
            finally in.close()
          }
          .mkString("\n")
      } finally zip.close()

    assert(
      text.contains(relocatedRef),
      "no class under org/archive/webservices/sparkling/http/ references the relocated " +
        "BrotliCompressorInputStream -- check the org.apache.commons.compress. relocation in the " +
        "shade-plugin config")
    assert(
      !text.contains(originalRef),
      "a class under org/archive/webservices/sparkling/http/ still references the un-relocated " +
        "BrotliCompressorInputStream -- the relocation did not take effect for Sparkling's " +
        "bundled classes")
  }
}
