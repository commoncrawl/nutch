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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.protocol.Content;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestWarcCdxWriter {

  private static final ObjectMapper JSON = new ObjectMapper();

  /**
   * Create a WarcCdxWriter writing to in-memory byte arrays.
   */
  private WarcCdxWriter createWriter(ByteArrayOutputStream warcOut,
      ByteArrayOutputStream cdxOut) {
    return new WarcCdxWriter(warcOut, cdxOut, new Path("/test/warc.gz"), null);
  }

  /**
   * Create a Content object with the required metadata fields.
   */
  private Content createContent(String statusCode, String contentType) {
    Content content = new Content();
    content.setContentType(contentType);
    Metadata meta = new Metadata();
    meta.set("HTTP-Status-Code", statusCode);
    meta.set("Content-Type", contentType);
    content.setMetadata(meta);
    return content;
  }

  /**
   * Parse the JSON portion from a CDX line (skip SURT and timestamp).
   */
  @SuppressWarnings("unchecked")
  private Map<String, String> parseCdxJson(String cdxLine) throws Exception {
    // CDX format: SURT TIMESTAMP JSON
    int firstSpace = cdxLine.indexOf(' ');
    int secondSpace = cdxLine.indexOf(' ', firstSpace + 1);
    String jsonPart = cdxLine.substring(secondSpace + 1);
    return JSON.readValue(jsonPart, Map.class);
  }

  @Test
  public void testWriteCdxLineWithIpAndRecordId() throws Exception {
    ByteArrayOutputStream warcOut = new ByteArrayOutputStream();
    ByteArrayOutputStream cdxOut = new ByteArrayOutputStream();
    WarcCdxWriter writer = createWriter(warcOut, cdxOut);

    URI targetUri = new URI("https://example.com/page");
    Date date = new Date();
    Content content = createContent("200", "text/html");
    String recordId = "urn:uuid:12345678-1234-1234-1234-123456789abc";
    String ip = "93.184.216.34";

    writer.writeCdxLine(targetUri, date, 0, 1000, "sha1:ABC123", content,
        false, null, null, recordId, ip);

    String cdxLine = cdxOut.toString(StandardCharsets.UTF_8).trim();
    Map<String, String> json = parseCdxJson(cdxLine);

    assertEquals("93.184.216.34", json.get("ipaddress"));
    assertEquals("12345678-1234-1234-1234-123456789abc", json.get("recordid"));
    assertEquals("https://example.com/page", json.get("url"));
    assertEquals("200", json.get("status"));
    assertEquals("text/html", json.get("mime"));
  }

  @Test
  public void testWriteCdxLineWithNullIpAndRecordId() throws Exception {
    ByteArrayOutputStream warcOut = new ByteArrayOutputStream();
    ByteArrayOutputStream cdxOut = new ByteArrayOutputStream();
    WarcCdxWriter writer = createWriter(warcOut, cdxOut);

    URI targetUri = new URI("https://example.com/page");
    Date date = new Date();
    Content content = createContent("200", "text/html");

    writer.writeCdxLine(targetUri, date, 0, 500, "sha1:DEF456", content,
        false, null, null, null, null);

    String cdxLine = cdxOut.toString(StandardCharsets.UTF_8).trim();
    Map<String, String> json = parseCdxJson(cdxLine);

    assertFalse(json.containsKey("ipaddress"),
        "ipaddress should not be present when ip is null");
    assertFalse(json.containsKey("recordid"),
        "recordid should not be present when recordId is null");
    assertEquals("https://example.com/page", json.get("url"));
  }

  @Test
  public void testRecordIdPrefixStripping() throws Exception {
    ByteArrayOutputStream warcOut = new ByteArrayOutputStream();
    ByteArrayOutputStream cdxOut = new ByteArrayOutputStream();
    WarcCdxWriter writer = createWriter(warcOut, cdxOut);

    URI targetUri = new URI("https://example.com/test");
    Date date = new Date();
    Content content = createContent("200", "text/html");
    String recordId = "urn:uuid:abcd-1234";

    writer.writeCdxLine(targetUri, date, 100, 2000, "sha1:XYZ789", content,
        false, null, null, recordId, "10.0.0.1");

    String cdxLine = cdxOut.toString(StandardCharsets.UTF_8).trim();
    Map<String, String> json = parseCdxJson(cdxLine);

    assertEquals("abcd-1234", json.get("recordid"),
        "urn:uuid: prefix should be stripped from record ID");
  }

  @Test
  public void testRevisitCdxLine() throws Exception {
    ByteArrayOutputStream warcOut = new ByteArrayOutputStream();
    ByteArrayOutputStream cdxOut = new ByteArrayOutputStream();
    WarcCdxWriter writer = createWriter(warcOut, cdxOut);

    URI targetUri = new URI("https://example.com/revisit");
    Date date = new Date();
    Content content = createContent("304", "application/http;msgtype=response");

    writer.writeCdxLine(targetUri, date, 0, 300, null, content, true, null,
        null, "urn:uuid:rev-id-001", "192.168.1.1");

    String cdxLine = cdxOut.toString(StandardCharsets.UTF_8).trim();
    Map<String, String> json = parseCdxJson(cdxLine);

    assertEquals("warc/revisit", json.get("mime"),
        "revisit record should have mime warc/revisit");
    assertFalse(json.containsKey("digest"),
        "revisit with null payloadDigest should not have digest");
    assertEquals("rev-id-001", json.get("recordid"));
    assertEquals("192.168.1.1", json.get("ipaddress"));
    assertEquals("304", json.get("status"));
  }
}
