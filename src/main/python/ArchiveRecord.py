# Archives Unleashed Toolkit (AUT):
# An open-source platform for analyzing web archives.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ExtractDate import ExtractDate, DateComponent

class ArchiveRecord(object):
  def __init__(self, contentBytes, contentString, crawlDate, crawlMonth, 
    domain, imageBytes, mimeType, url):
    self.contentBytes = contentBytes
    self.contentString = contentString
    self.crawlDate = contentString
    self.crawlMonth = crawlMonth
    self.domain = domain
    self.imageBytes = imageBytes
    self.mimeType = mimeType
    self.url = url

  def getCrawlDate(self): 
    return self.crawlDate

  def getMimeType(self):
    return self.mimeType

  def getUrl(self):
    return self.url 

  def getContentBytes(self):
    return self.contentBytes

  def getCrawlMonth(self):
    return ExtractDate(self.crawlDate, DateComponent.YYYYMM)

  def getDomain(self):
    return self.domain

  def getContentBytes(self):
    return self.contentBytes

  def getContentString(self):
    return self.contentString

  def getImageBytes(self):
    return self.imageBytes

