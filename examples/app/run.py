#!/usr/bin/env python

# Copyright 2017 Google Inc.
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

"""This is a demo for V3 features.

The script will launch all the processes necessary to bring up
the demo. It will bring up an HTTP server on port 8000 by default,
which you can override. Once done, hitting <Enter> will terminate
all processes. Vitess will always be started on port 12345.
"""

import thread

from CGIHTTPServer import CGIHTTPRequestHandler
from BaseHTTPServer import HTTPServer


httpd = HTTPServer(('', 8000), CGIHTTPRequestHandler)
httpd.serve_forever()
