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

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.IDN;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for how OkHttp parses and normalizes hosts in three forms:
 *  - Unicode (e.g. "https://🧠.s.country/...")
 *  - Percent-encoded UTF-8 (e.g. "https://%F0%9F%A7%A0.s.country/...")
 *  - Punycode / ACE (e.g. "https://xn--nv8h.s.country/...")
 */
public class TestOkHttpPunyCodeNormalization {

    // U+1F9E0 BRAIN
    private static final String BRAIN_UNICODE   = "🧠";
    private static final String BRAIN_PCT_UTF8  = "%F0%9F%A7%A0";
    private static final String BRAIN_PUNYCODE  = "xn--qv9h";

    private static final String PARENT = ".s.country";
    private static final String PATH   = "/p/human-protocol-aligning-hearts-bots";


    @Test
    public void unicodeHostNormalizesToPunycode() {
        HttpUrl url = HttpUrl.parse("https://" + BRAIN_UNICODE + PARENT + PATH);
        assertNotNull(url, "HttpUrl.parse must accept Unicode host");
        assertEquals(BRAIN_PUNYCODE + PARENT, url.host());
    }

    @Test
    public void percentEncodedHostNormalizesToPunycode() {
        // This is the CC WARC-Target-URI form. The question: does OkHttp
        // decode the percent-escapes in the host and IDN-normalize, or
        // does it leave them as literal characters / mis-normalize?
        HttpUrl url = HttpUrl.parse("https://" + BRAIN_PCT_UTF8 + PARENT + PATH);
        assertNotNull(url, "HttpUrl.parse must accept percent-encoded host");
        assertEquals(
            BRAIN_PUNYCODE + PARENT, url.host(), "Percent-encoded UTF-8 host must normalize to Punycode for the SAME emoji");
    }

    @Test
    public void punycodeHostPassesThrough() {
        HttpUrl url = HttpUrl.parse("https://" + BRAIN_PUNYCODE + PARENT + PATH);
        assertNotNull(url);
        assertEquals(BRAIN_PUNYCODE + PARENT, url.host());
    }

    @Test
    public void allThreeFormsProduceEquivalentHost() {
        HttpUrl uni = HttpUrl.parse("https://" + BRAIN_UNICODE  + PARENT + PATH);
        HttpUrl pct = HttpUrl.parse("https://" + BRAIN_PCT_UTF8 + PARENT + PATH);
        HttpUrl ace = HttpUrl.parse("https://" + BRAIN_PUNYCODE + PARENT + PATH);
        assertNotNull(uni);
        assertNotNull(pct);
        assertNotNull(ace);
        assertEquals(uni.host(), pct.host());
        assertEquals(pct.host(), ace.host());
    }

    @Test
    public void pathIsNotMangledByHostNormalization() {
        // Sanity: percent-decoding the host must not bleed into the path.
        HttpUrl url = HttpUrl.parse("https://" + BRAIN_PCT_UTF8 + PARENT + PATH);
        assertNotNull(url);
        assertEquals(PATH, url.encodedPath());
    }

    @Test
    public void javaIdnAgreesWithOkHttp() {
        // Cross-check OkHttp's host() output against the JDK's IDN.toASCII()
        // so we know which spec OkHttp is following.
        String jdk = IDN.toASCII(BRAIN_UNICODE + PARENT, IDN.ALLOW_UNASSIGNED);
        HttpUrl url = HttpUrl.parse("https://" + BRAIN_UNICODE + PARENT + PATH);
        assertNotNull(url);
        assertEquals(jdk, url.host());
    }


    @Test
    public void hostHeaderMatchesNormalizedHost() throws IOException {
        // Build a request and intercept it BEFORE it hits the network, so
        // we can read the exact Host header OkHttp would send. We use an
        // application interceptor that short-circuits with a synthetic
        // response — no actual DNS / TCP needed.
        AtomicReference<String> seenHost   = new AtomicReference<>();
        AtomicReference<String> seenUrl    = new AtomicReference<>();

        Interceptor capture = chain -> {
            Request req = chain.request();
            seenHost.set(req.header("Host") != null
                ? req.header("Host")
                : req.url().host()); // OkHttp adds Host at the network layer
            seenUrl.set(req.url().toString());
            return new Response.Builder()
                .request(req)
                .protocol(okhttp3.Protocol.HTTP_1_1)
                .code(204)
                .message("No Content (synthetic)")
                .build();
        };

        OkHttpClient client = new OkHttpClient.Builder()
            .addInterceptor(capture)
            .callTimeout(2, TimeUnit.SECONDS)
            .build();

        String input = "https://" + BRAIN_PCT_UTF8 + PARENT + PATH;
        Request req  = new Request.Builder().url(input).head().build();
        try (Response r = client.newCall(req).execute()) {
            assertEquals(204, r.code());
        }

        assertEquals(
            BRAIN_PUNYCODE + PARENT, seenHost.get(),
                "Effective host derived from a percent-encoded UTF-8 input must be the matching Punycode");
    }

    // -- Mismatch detector (the CC bug, reproduced if it triggers) -----------

    @Test
    public void parsedHostMustMatchOriginalEmoji() {
        // If this ever fails, OkHttp itself is producing a host that
        // disagrees with the input — which would be the CC WARC bug
        // happening inside OkHttp. Currently expected to pass.
        String[] inputs = {
            "https://" + BRAIN_UNICODE  + PARENT + PATH,
            "https://" + BRAIN_PCT_UTF8 + PARENT + PATH,
            "https://" + BRAIN_PUNYCODE + PARENT + PATH,
        };
        for (String s : inputs) {
            HttpUrl u = HttpUrl.parse(s);
            assertNotNull(u, "parse failed for " + s);
            assertTrue(
                    u.host().startsWith(BRAIN_PUNYCODE + "."),
                    "Host for " + s + " was " + u.host() + ", expected to contain " + BRAIN_PUNYCODE);
        }
    }
}
