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
package org.commoncrawl.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Test;

public class TestWarcWriter {

  @Test
  public void testWriteRevisitRecordContentType() throws IOException, URISyntaxException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WarcWriter writer = new WarcWriter(bos);

    byte[] block = "HTTP/1.1 304\r\ndate: Fri, 06 Feb 2026 10:55:35 GMT\r\n\r\n".getBytes();

    Configuration conf = NutchConfiguration.create();
    Metadata metadata = new Metadata();
    metadata.add("Content-Type", "text/html");
    Content content = new Content("https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025", "https://de.wikipedia.org",
        block, "text/html", metadata, conf);

    URI targetUri = new URI("https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025");
    String ip = "208.80.154.224";
    int httpStatusCode = 304;
    java.util.Date date = new java.util.Date();
    URI warcinfoId = writer.getRecordId();
    URI relatedId = writer.getRecordId();
    String warcProfile = WarcWriter.PROFILE_REVISIT_IDENTICAL_DIGEST;
    java.util.Date refersToDate = new java.util.Date(System.currentTimeMillis() - 3600000);
    String payloadDigest = "sha1:abc123";
    String blockDigest = "sha1:def456";

    writer.writeWarcRevisitRecord(targetUri, ip, httpStatusCode, date,
        warcinfoId, relatedId, warcProfile, refersToDate, payloadDigest,
        blockDigest, null, null, block, content);

    byte[] compressed = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
    gis.transferTo(decompressed);

    String warcOutput = decompressed.toString();
    System.out.println(warcOutput);

    assertTrue(warcOutput.contains("WARC-Type: revisit"),
        "WARC record should have WARC-Type: revisit");
    assertTrue(warcOutput.contains("Content-Type: application/http; msgtype=response"),
        "WARC revisit record should have Content-Type: application/http; msgtype=response");
    assertTrue(warcOutput.contains("WARC-Refers-To-Target-URI: https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025"),
        "WARC record should have WARC-Refers-To-Target-URI header");
    assertTrue(warcOutput.contains("WARC-Profile: " + warcProfile),
        "WARC record should have WARC-Profile header");
  }
}
