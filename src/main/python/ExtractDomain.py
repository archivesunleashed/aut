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

from urllib.parse import urlparse

def ExtractDomain(url, source = ""):
  if url is None: 
    return None
  host = None
  try:
    host = urlparse(url).hostname
  except Exception as e:
    # it's okay 
    pass 
  if (host is not None or source == ""): 
    return host
  try:
    host = urlparse(source).hostname
    return host
  except Exception as e:
    return None



