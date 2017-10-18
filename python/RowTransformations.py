from ExtractDomain import ExtractDomain
from ExtractDate import DateComponent, ExtractDate
from langdetect import detect
import re


def countItems(rdd): 
  return rdd.map(lambda r: (r, 1)) \
    .reduceByKey(lambda c1, c2: c1 + c2) \
    .sortBy(lambda f: f[1], ascending = False)

def keepValidPages(row):
  return (row['crawlDate'] is not None 
      and (row['mimeType'] == "text/html"
      or row['mimeType'] == "application/xhtml+xml"
      or row['url'].endswith("htm")
      or row['url'].endswith("html"))
      and not row['url'].endswith("robots.txt"))

def keepImages(row): 
  return (row['crawlDate'] is not None
      and (
      (row['mimeType'] is not None and "image/" in row['mimeType'])
      or row['url'].endswith("jpg")
      or row['url'].endswith("jpeg")
      or row['url'].endswith("png"))
      and not row['url'].endswith("robots.txt"))
    
def keepMimeTypes(row, mimeTypes):
  return row['mimeType'] in mimeTypes
    
def keepDate(row, date, component = DateComponent.YYYYMMDD):
  return ExtractDate(row['crawlDate'], component) == date    

def keepUrls(row, urls):
  return row['url'] in urls
    
def keepUrlPatterns(row, urlREs):
  # TODO: Verify that this is correct
  url = row['url']
  for pattern in urlREs:
    if re.match(pattern, url) is not None:
      return True 
  return False
    
def keepDomains(row, urls): 
  # TODO: Verify that this is correct
  return re.sub(r"^\\s*www\\.", "", ExtractDomain(row['url'])) in urls

def keepLanguages(row, langs):
  return detect(RemoveHTML(row.contentString)) in langs

def keepContent(row, contentREs):
  # TODO: Verify that this is correct
  contents = row['contentString']
  for pattern in contentREs:
    if re.match(pattern, contents) is not None:
      return True
  return False

def discardMimeTypes(row, mimeTypes): 
  return not keepMimeTypes(row, mimeTypes)

def discardDate(row, date):
  return not keepDate(row, date)

def discardUrls(row, urls):
  return not keepUrls(row, urls)

def discardUrlPatterns(row, urlREs):
  return not keepUrlPatterns(row, urlREs)

def discardDomains(row, urls):
  return not keepDomains(row, urls)

def discardContent(row, contentREs):
  return not discardContent(row, contentREs)
    
  


