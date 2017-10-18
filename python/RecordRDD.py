# Archives Unleashed Toolkit (AUT):
# An open-source platform for analyzing web archives.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.rdd import RDD

from pyspark.rdd import RDD, PipelinedRDD

class RecordRDD(RDD):
  def __init__(self, rdd, first = False):
    self._jrdd = rdd._jrdd
    self.is_cached = rdd.is_cached
    self.is_checkpointed = rdd.is_checkpointed
    self.ctx = rdd.ctx
    self._jrdd_deserializer = rdd._jrdd_deserializer
    self._id = rdd._id
    self.partitioner = rdd.partitioner

  def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
      return RecordRDD(PipelinedRDD(self, f, preservesPartitioning), False)

  def union(self, other):
      return RecordRDD(super(RecordRDD, self).union(other), False)

  def countItems(self):
    return self.map(lambda r : (r, 1)) \
      .reduceByKey(lambda c1, c2 : c1 + c2) \
      .sortBy(lambda f: f[1], ascending = False)

  def keepValidPages(self):
    return self.filter(lambda r:
      r.getCrawlDate() is not None
        and (r.getMimeType() == "text/html"
        or r.getMimeType() == "application/xhtml+xml"
        or r.getUrl().endswith("htm")
        or r.getUrl().endswith("html"))
        and not r.getUrl().endswith("robots.txt"))

  def keepImages(self):
    return self.filter(lambda r:
      r.getCrawlDate() != None
        and (
        (r.getMimeType != null and "image/" in r.getMimeType())
        or r.getUrl.endswith("jpg")
        or r.getUrl.endswith("jpeg")
        or r.getUrl.endswith("png"))
        and not r.getUrl.endswith("robots.txt"))

  def keepMimeTypes(self, mimeTypes):
    return self.filter(lambda r: r.getMimeType() in mimeTypes)

def countItems(rdd):
  return rdd.map(lambda r : (r, 1)) \
    .reduceByKey(lambda c1, c2 : c1 + c2) \
    .sortBy(lambda f: f[1], ascending = False)


# rdd: An rdd of ArcRecords
def keepValidPages(rdd):
  return rdd.filter(lambda r:
    r.getCrawlDate() is not None
      and (r.getMimeType() == "text/html"
      or r.getMimeType() == "application/xhtml+xml"
      or r.getUrl().endswith("htm")
      or r.getUrl().endswith("html"))
      and not r.getUrl().endswith("robots.txt"))

# rdd: An rdd of Rows
def keepValidPages2(rdd):
  return rdd.filter(lambda r:
    r.crawlDate is not None
      and (r.mimeType == "text/html"
      or r.mimeType == "application/xhtml+xml"
      or r.url.endswith("htm")
      or r.url.endswith("html"))
      and not r.url.endswith("robots.txt"))

def keepImages(rdd):
  return rdd.filter(lambda r:
    r.getCrawlDate() != None
      and (
      (r.getMimeType != null and "image/" in r.getMimeType())
      or r.getUrl.endswith("jpg")
      or r.getUrl.endswith("jpeg")
      or r.getUrl.endswith("png"))
      and not r.getUrl.endswith("robots.txt"))

def keepMimeTypes(rdd, mimeTypes):
  return rdd.filter(lambda r: r.getMimeType() in mimeTypes)


# def keepDate(date: String, component: DateComponent = DateComponent.YYYYMMDD) = {
#   rdd.filter(r => ExtractDate(r.getCrawlDate, component) == date)
# }

#     def keepUrls(urls: Set[String]) = {
#       rdd.filter(r => urls.contains(r.getUrl))
#     }

#     def keepUrlPatterns(urlREs: Set[Regex]) = {
#       rdd.filter(r =>
#         urlREs.map(re =>
#           r.getUrl match {
#             case re() => true
#             case _ => false
#           }).exists(identity))
#     }

#     def keepDomains(urls: Set[String]) = {
#       rdd.filter(r => urls.contains(ExtractDomain(r.getUrl).replace("^\\s*www\\.", "")))
#     }

#     def keepLanguages(lang: Set[String]) = {
#       rdd.filter(r => lang.contains(DetectLanguage(RemoveHTML(r.getContentString))))
#     }

#     def keepContent(contentREs: Set[Regex]) = {
#       rdd.filter(r =>
#         contentREs.map(re =>
#           (re findFirstIn r.getContentString) match {
#             case Some(v) => true
#             case None => false
#           }).exists(identity))
#     }

#     def discardMimeTypes(mimeTypes: Set[String]) = {
#       rdd.filter(r => !mimeTypes.contains(r.getMimeType))
#     }

#     def discardDate(date: String) = {
#       rdd.filter(r => r.getCrawlDate != date)
#     }

#     def discardUrls(urls: Set[String]) = {
#       rdd.filter(r => !urls.contains(r.getUrl))
#     }

#     def discardUrlPatterns(urlREs: Set[Regex]) = {
#       rdd.filter(r =>
#         !urlREs.map(re =>
#           r.getUrl match {
#             case re() => true
#             case _ => false
#           }).exists(identity))
#     }

#     def discardDomains(urls: Set[String]) = {
#       rdd.filter(r => !urls.contains(r.getDomain))
#     }

#     def discardContent(contentREs: Set[Regex]) = {
#       rdd.filter(r =>
#         !contentREs.map(re =>
#           (re findFirstIn r.getContentString) match {
#             case Some(v) => true
#             case None => false
#           }).exists(identity))
#     }
#   }

# }
