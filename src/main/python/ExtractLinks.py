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

from bs4 import BeautifulSoup
from urllib.parse import urljoin

def ExtractLinks(src, html, base = ""):
  output = list()

  # Basic input checking, return empty list if we fail.
  if (src is None or len(src) == 0): return output
  if (html is None or len(html) == 0): return output

  soup = BeautifulSoup(html, 'html.parser')
  links = soup.find_all('a', href = True)
  for link in links:
    target = link['href']
    if base != "":
      target = urljoin(base, target)
    if target is not None and target != "": 
      output.append((src, target, link.text))
  return output
  
  
