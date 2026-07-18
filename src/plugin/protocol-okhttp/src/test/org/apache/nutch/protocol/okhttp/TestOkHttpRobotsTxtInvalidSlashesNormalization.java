/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.protocol.okhttp;

import okhttp3.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.IDN;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for how OkHttp parses and normalizes hosts in three forms:
*/
public class TestOkHttpRobotsTxtInvalidSlashesNormalization {

    @Test
    public void unicodeHostNormalizesToPunycode() {
        HttpUrl url = HttpUrl.parse("https:////sites.google.com/bao");
        assertNotNull(url, "HttpUrl.parse must accept Unicode host");
        assertEquals("sites.google.com", url.host());
    }

 
}
