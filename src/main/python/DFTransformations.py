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

from ExtractDomain import ExtractDomain
from ExtractDate import DateComponent, ExtractDate
from RemoveHTML import RemoveHTML
from DetectLanguage import DetectLanguage
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re

def countItems(rdd):
  return rdd.map(lambda r: (r, 1)) \
    .reduceByKey(lambda c1, c2: c1 + c2) \
    .sortBy(lambda f: f[1], ascending = False)

def keepImages(df):
  return df.filter(df['crawlDate'].isNotNull()
      & (
      (df['mimeType'].isNotNull() & df['mimeType'].like("%image/%"))
      | df['url'].endswith("jpg")
      | df['url'].endswith("jpeg")
      | df['url'].endswith("png"))
      & ~df['url'].endswith("robots.txt"))

def keepMimeTypes(df, mimeTypes):
  return df.filter(df['mimeType'].isin(mimeTypes))

def keepDate(df, date, component = DateComponent.YYYYMMDD):
  def date_filter(d):
    return ExtractDate(d, component) == date
  date_filter_udf = udf(date_filter, BooleanType())
  return df.filter(date_filter_udf(df['crawlDate']))

def keepUrls(df, urls):
  return df.filter(df['url'].isin(urls))

def keepUrlPatterns(df, urlREs):
  def url_filter(url):
    for pattern in urlREs:
      if re.match(pattern, url) is not None:
        return True
    return False
  url_filter_udf = udf(url_filter, BooleanType())
  return df.filter(url_filter_udf(df['url']))

def keepDomains(df, urls):
  def domain_filter(url):
    return re.sub(r"^\\s*www\\.", "", ExtractDomain(url)) in urls
  domain_filter_udf = udf(domain_filter, BooleanType())
  return df.filter(domain_filter_udf(df['url']))

def keepLanguages(df, langs):
  def content_filter(contentString):
    return DetectLanguage(RemoveHTML(contentString)) in langs
  content_filter_udf = udf(content_filter, BooleanType())
  return df.filter(content_filter_udf(df['contentString']))

def keepContent(df, contentREs):
  def content_filter(content):
    for pattern in contentREs:
      if re.match(pattern, content) is not None:
        return True
    return False
  content_filter_udf = udf(content_filter, BooleanType())
  return df.filter(content_filter_udf(df['contentString']))

def discardMimeTypes(df, mimeTypes):
  return df.filter(~df['mimeType'].isin(mimeTypes))

def discardDate(df, date, component = DateComponent.YYYYMMDD):
  def date_filter(d):
    return ExtractDate(d, component) == date
  date_filter_udf = udf(date_filter, BooleanType())
  return df.filter(~date_filter_udf(df['crawlDate']))

def discardUrls(df, urls):
  return df.filter(~df['url'].isin(urls))

def discardUrlPatterns(df, urlREs):
  def url_filter(url):
    for pattern in urlREs:
      if re.match(pattern, url) is not None:
        return True
    return False
  url_filter_udf = udf(url_filter, BooleanType())
  return df.filter(~url_filter_udf(df['url']))

def discardDomains(df, urls):
  def domain_filter(url):
    return re.sub(r"^\\s*www\\.", "", ExtractDomain(url)) in urls
  domain_filter_udf = udf(domain_filter, BooleanType())
  return df.filter(~domain_filter_udf(df['url']))

def discardContent(df, contentREs):
  def content_filter(content):
    for pattern in contentREs:
      if re.match(pattern, content) is not None:
        return True
    return False
  content_filter_udf = udf(content_filter, BooleanType())
  return df.filter(~content_filter_udf(df['contentString']))
