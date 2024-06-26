<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Updates
* The use of phantomjs has been deprecated. Check [Wikipedia](https://en.wikipedia.org/wiki/PhantomJS) for more info.
* The updated code for Safari webriver is under development as starting Safari 10 on OS X El Capitan and macOS Sierra, Safari comes bundled with a new driver implementation.
* Opera is now based on ChromeDriver and has been adapted by Opera that enables programmatic automation of Chromium-based Opera products but hasn't been updated since April 5, 2017. We have suspended its support and removed from the code.([link](https://github.com/operasoftware/operachromiumdriver)) 
* Headless mode has been added for Chrome and Firefox. Set `selenium.enable.headless` to `true` in nutch-default.xml or nutch-site.xml to use it.


Your can run Nutch in Docker.  Check  some examples at https://github.com/sbatururimi/nutch-test.
Don't forget to update Dockerfile to point to the original Nutch repository when updated.

# Contributors
Stas Batururimi [s.batururimi@gmail.com]

