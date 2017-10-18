from bs4 import BeautifulSoup

def ExtractLinks(src, html, base = ""):
  output = list()

  # Basic input checking, return empty list if we fail.
  if (src is None or len(src) == 0): return output
  if (html is None or len(html) == 0): return output

  soup = BeautifulSoup(html, 'html.parser')
  links = soup.find_all('a', href = True)
  for link in links:
    # TODO: What does this mean? Couldn't find a similar method in bs4
    # if (base.nonEmpty) link.setBaseUri(base)
    target = link['href']
    if target is not None and target != "": 
      output.append((src, target, link.text))
  return output
  
  
