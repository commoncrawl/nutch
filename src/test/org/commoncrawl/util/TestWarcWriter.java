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

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.protocol.Content;
import org.commoncrawl.util.test.SegmenterRecordReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestWarcWriter {

    @Test
    public void testWriteRevisitRecordContentType() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        WarcWriter writer = new WarcWriter(bos);

        File segmentDir = new File(System.getProperty("test.build.data", "src/testresources"), "test-segments/20260224170658-revisit");
        assertNotNull(segmentDir, "Missing segment resource");
        String segmentPath = segmentDir.getAbsolutePath();
        String url = "https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025";

        Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);
        assertThat("Revisit record should not have any payload or content",
                content.getContent(), is(new byte[]{}));
        URI targetUri = new URI(content.getUrl());

        Metadata metadata = content.getMetadata();
        String ip = content.getMetadata().get("_ip_");
        int httpStatusCode = 304;

        Date date = HttpDateFormat.toDate(metadata.get("date"));
        URI warcinfoId = writer.getRecordId(date.getTime());
        URI relatedId = writer.getRecordId(date.getTime());
        String warcProfile = WarcWriter.PROFILE_REVISIT_IDENTICAL_DIGEST;
        Date refersToDate = new Date(System.currentTimeMillis() - 3600000);
        String payloadDigest = "sha1:abc123";
        String blockDigest = "sha1:def456";

        writer.writeWarcRevisitRecord(targetUri, ip, httpStatusCode, date,
                warcinfoId, relatedId, warcProfile, refersToDate, payloadDigest,
                blockDigest, null, null, content.getContent(), content);

        byte[] compressed = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(bis);
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        gis.transferTo(decompressed);

        String warcOutput = decompressed.toString();

        assertTrue(warcOutput.contains("WARC-Type: revisit"),
                "WARC record should have WARC-Type: revisit");
        assertTrue(warcOutput.contains("Content-Type: application/http; msgtype=response"),
                "WARC revisit record should have Content-Type: application/http; msgtype=response");
        assertTrue(warcOutput.contains("WARC-Refers-To-Target-URI: https://de.wikipedia.org/wiki/Wikipedia:WikiCon_2025"),
                "WARC record should have WARC-Refers-To-Target-URI header");
        assertTrue(warcOutput.contains("WARC-Profile: " + warcProfile),
                "WARC record should have WARC-Profile header");
    }

    @Test
    public void testWriteRecordWithUUID7() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        WarcWriter writer = new WarcWriter(bos);

        File segmentDir = new File(System.getProperty("test.build.data", "src/testresources"), "test-segments/20150309101656");
        assertNotNull(segmentDir, "Missing segment resource");
        String segmentPath = segmentDir.getAbsolutePath();

        String url = "http://avro.apache.org/";
        Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);

        URI targetUri = new URI(content.getUrl());
        Metadata metadata = content.getMetadata();
        String ip = content.getMetadata().get("_ip_");
        String contentType = content.getMetadata().get("Content-Type");
        int httpStatusCode = 200;

        Date date = HttpDateFormat.toDate(metadata.get("Date"));
        URI warcinfoId = writer.getRecordId(date.getTime());
        String payloadDigest = "sha1:abc123";
        String blockDigest = "sha1:def456";

        URI recordId = writer.getRecordId(date.getTime());

        writer.writeWarcinfoRecord("output", "avro.apache.org", "CCF", "CCF", "CCBot", warcinfoId.toString(), "blablabal", date);
        writer.writeWarcRequestRecord(targetUri, ip, date, warcinfoId, null, null, content.getContent());
        writer.writeWarcResponseRecord(targetUri, ip, httpStatusCode, date, warcinfoId, recordId, payloadDigest, blockDigest, "False", null, null, content.getContent(), content);

        byte[] compressed = bos.toByteArray();
        ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
        StringBuilder allRecords = new StringBuilder();
        while (bis.available() > 0) {
            GZIPInputStream gis = new GZIPInputStream(bis);
            ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
            gis.transferTo(decompressed);
            allRecords.append(decompressed.toString());
        }
        String warcOutput = allRecords.toString();

        Pattern pattern = Pattern.compile("WARC-Record-ID: <urn:uuid:([^>]+)>");
        Matcher matcher = pattern.matcher(warcOutput);
        List<UUID> recordIds = new ArrayList<>();
        while (matcher.find()) {
            recordIds.add(UUID.fromString(matcher.group(1)));
        }

        assertEquals(3, recordIds.size(), "should have 3 WARC-Record-IDs");

        assertEquals(3, new HashSet<>(recordIds).size(),
                "all record IDs must be unique");

        long expectedTs = date.getTime();
        for (UUID uuid : recordIds) {
            assertEquals(7, uuid.version(), "must be UUIDv7");
            assertEquals(2, uuid.variant(), "must be IETF variant");
            long embedded = uuid.getMostSignificantBits() >>> 16;
            assertEquals(expectedTs, embedded,
                    "timestamp must match capture date");
        }
    }

  @Test
  @Disabled("This test is testing a behaviour we are not sure we will implement - fixing the issue downstream instead of upstream. ")
  public void testWriteResponseRecordWithMalformedURL() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WarcWriter writer = new WarcWriter(bos);

    File segmentDir = new File(System.getProperty("test.build.data", "."),
            "test-segments/20260505091103-malformed-urls");
    assertNotNull(segmentDir, "Missing segment resource");
    String segmentPath = segmentDir.getAbsolutePath();
    String url = "https:////sites.google.com/site/lebercailgiteennormandie/robots.txt";

    Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);
    assert (content.getContent() != null && content.getContent().length > 0) : "Content in fetched 200s records must not be null.";
    URI targetUri = new URI(content.getUrl());

    Metadata metadata = content.getMetadata();
    String ip = content.getMetadata().get("_ip_");
    int httpStatusCode = 200;

    Date date = HttpDateFormat.toDate(metadata.get("date"));
    URI warcinfoId = writer.getRecordId();
    URI relatedId = writer.getRecordId();
    String payloadDigest = "sha1:abc123";
    String blockDigest = "sha1:def456";

    writer.writeWarcResponseRecord(targetUri, ip, httpStatusCode, date,
        warcinfoId, relatedId, payloadDigest,
        blockDigest, "false",
            null,
            null, content.getContent(), content);

    byte[] compressed = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
    gis.transferTo(decompressed);

    String warcOutput = decompressed.toString();

    assertTrue(warcOutput.contains("WARC-Target-URI: https://sites.google.com/site/lebercailgiteennormandie/robots.txt"),
        "WARC-Target-URI should be normalized to a valid URL");
  }

  @Test
  @Disabled("This test is testing a behaviour we are not sure we will implement - fixing the issue downstream instead of upstream. ")
  public void testWriteRequestRecordWithMalformedURL() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WarcWriter writer = new WarcWriter(bos);

    File segmentDir = new File(System.getProperty("test.build.data", "."),
            "test-segments/20260505091103-malformed-urls");
    assertNotNull(segmentDir, "Missing segment resource");
    String segmentPath = segmentDir.getAbsolutePath();
    String url = "https:////sites.google.com/site/lebercailgiteennormandie/robots.txt";

    Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);
    assert (content.getContent() != null && content.getContent().length > 0) : "Content in fetched 200s records must not be null.";
    URI targetUri = new URI(content.getUrl());

    Metadata metadata = content.getMetadata();
    String ip = content.getMetadata().get("_ip_");

    Date date = HttpDateFormat.toDate(metadata.get("date"));
    URI warcinfoId = writer.getRecordId();

    writer.writeWarcRequestRecord(targetUri, ip, date,
        warcinfoId, null, null, content.getContent());

    byte[] compressed = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
    gis.transferTo(decompressed);

    String warcOutput = decompressed.toString();

    assertTrue(warcOutput.contains("WARC-Target-URI: https://sites.google.com/site/lebercailgiteennormandie/robots.txt"),
        "WARC-Target-URI should be normalized to a valid URL");
  }

  @Test
  @Disabled("This test is testing a behaviour we are not sure we will implement - fixing the issue downstream instead of upstream. ")
  public void testWriteMetadataRecordWithMalformedURL() throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WarcWriter writer = new WarcWriter(bos);

    File segmentDir = new File(System.getProperty("test.build.data", "."),
            "test-segments/20260505091103-malformed-urls");
    assertNotNull(segmentDir, "Missing segment resource");
    String segmentPath = segmentDir.getAbsolutePath();
    String url = "https:////sites.google.com/site/lebercailgiteennormandie/robots.txt";

    Content content = SegmenterRecordReader.retrieveContent(segmentPath, url);
    assert (content.getContent() != null && content.getContent().length > 0) : "Content in fetched 200s records must not be null.";
    URI targetUri = new URI(content.getUrl());

    Metadata metadata = content.getMetadata();
    URI relatedId = writer.getRecordId();
    String blockDigest = "sha1:def456";

    Date date = HttpDateFormat.toDate(metadata.get("date"));
    URI warcinfoId = writer.getRecordId();

    writer.writeWarcMetadataRecord(targetUri, date, warcinfoId, relatedId, blockDigest, content.getContent());

    byte[] compressed = bos.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
    GZIPInputStream gis = new GZIPInputStream(bis);
    ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
    gis.transferTo(decompressed);

    String warcOutput = decompressed.toString();

    assertTrue(warcOutput.contains("WARC-Target-URI: https://sites.google.com/site/lebercailgiteennormandie/robots.txt"),
        "WARC-Target-URI should be normalized to a valid URL");
  }
}
