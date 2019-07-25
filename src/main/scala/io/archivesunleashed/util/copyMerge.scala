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
/*
 * Reimplementation of FileUtil.copyMerge for
 * https://github.com/archivesunleashed/aut/issues/329.
 *
 * Taken from https://stackoverflow.com/questions/42035735/how-to-do-copymerge-in-hadoop-3-0
 */
package io.archivesunleashed.util

import scala.util.Try
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import java.io.IOException

/** Reimplementation of FileUtil.copyMerge . */
object CopyMerge {
  def copyMerge(
    srcFS: FileSystem, srcDir: Path,
    dstFS: FileSystem, dstFile: Path,
    deleteSource: Boolean, conf: Configuration
  ): Boolean = {

    if (dstFS.exists(dstFile))
      throw new IOException(s"Target $dstFile already exists")

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory()) {
      val outputFile = dstFS.create(dstFile)
      Try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile() =>
              val inputFile = srcFS.open(status.getPath())
              Try(IOUtils.copyBytes(inputFile, outputFile, conf, false))
              inputFile.close()
          }
      }
      outputFile.close()
      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }
}
