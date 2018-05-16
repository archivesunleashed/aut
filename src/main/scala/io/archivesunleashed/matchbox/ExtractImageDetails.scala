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
package io.archivesunleashed.matchbox

import java.io.ByteArrayInputStream
import java.io.IOException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.image.{ImageParser, TiffParser};
import org.apache.tika.parser.jpeg.JpegParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;

/** Information about an image. e.g. width, height*/
class ImageDetails(w: String, h: String) {
	val width: String = w
	val height: String = h
}

/** Extracts image details given raw bytes (using Apache Tika) */
object ExtractImageDetails {

	/**
	 * @param bytes the raw bytes of the image
	 * @return A tuple containing the width and height of the image
	*/
	def apply(url: String, mimetype: String, bytes: Array[Byte]): ImageDetails = {
		val inputStream = new ByteArrayInputStream(bytes)
		val handler = new BodyContentHandler();
  	val metadata = new Metadata();
  	val pcontext = new ParseContext();

  	if ((mimetype != null && mimetype.contains("image/jpeg")) || url.endsWith("jpg") || url.endsWith("jpeg")) {
  		val parser = new JpegParser();
  		val results = parser.parse(inputStream, handler, metadata, pcontext)
  	} else if ((mimetype != null && mimetype.contains("image/tiff")) || url.endsWith("tiff")) {
  		val parser = new TiffParser();
  		val results = parser.parse(inputStream, handler, metadata, pcontext)
  	} else {
  		val parser = new ImageParser();
			val results = parser.parse(inputStream, handler, metadata, pcontext)
    }
    return new ImageDetails(metadata.get("Image Width"), metadata.get("Image Height"))
	}
}