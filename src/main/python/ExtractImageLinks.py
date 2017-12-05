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

# UDF for extracting image links from a webpage given the HTML content (using BeautifulSoup).
def ExtractImageLinks(src, html):
    output = list()
    if (html == ""): 
      return output
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select('img[src]')
    for link in links:
      target = link['src']
      abs_target = urljoin(src, target)
      output.append(abs_target)
    return output




