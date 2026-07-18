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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.commoncrawl.util.test.SegmenterRecordReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  /**
   * Drives the real {@link WarcRecordWriter#write} path (not the low-level
   * {@link WarcWriter} record methods) to prove that WARC-Target-URI is taken
   * from the effective URL carried on {@link Content#getBaseUrl()}, while the
   * malformed requested URL (the fetch key on {@code WarcCapture.url} /
   * {@code Content.getUrl()}) never becomes the target. The fixture segment
   * ships only the Content, so a minimal successful CrawlDatum is synthesized.
   */
  @Test
  public void testWriteRecordUsesEffectiveBaseUrlAsTargetUri() throws Exception {
    Configuration conf = NutchConfiguration.create();

    File segmentDir = new File(System.getProperty("test.build.data", "."),
        "test-segments/20260505091103-malformed-urls");
    assertNotNull(segmentDir, "Missing segment resource");
    String segmentPath = segmentDir.getAbsolutePath();

    // The fixture's Content is keyed by (and carries) the malformed requested URL.
    String requestedUrl = "https:////sites.google.com/site/lebercailgiteennormandie/robots.txt";
    String effectiveUrl = "https://sites.google.com/site/lebercailgiteennormandie/robots.txt";

    Content fixture = SegmenterRecordReader.retrieveContent(segmentPath,
        requestedUrl);
    assertTrue(fixture.getContent() != null && fixture.getContent().length > 0,
        "Content of a fetched 200 record must not be empty");

    // Rebuild the Content the way the patched protocol layer now does:
    // url = requested URL (fetch key), base = effective URL put on the wire.
    Content content = new Content(requestedUrl, effectiveUrl,
        fixture.getContent(), fixture.getContentType(), fixture.getMetadata(),
        conf);

    // The fixture ships only the Content, so synthesize a successful fetch datum.
    CrawlDatum datum = new CrawlDatum();
    datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
    datum.setFetchTime(System.currentTimeMillis());
    datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY,
        ProtocolStatus.STATUS_SUCCESS);
    datum.getMetaData().put(Nutch.PROTOCOL_STATUS_CODE_KEY, new Text("200"));

    Path outputPath = new Path(
        Files.createTempDirectory("warc-record-writer-test").toString());

    // WarcRecordWriter only touches the context to increment counters.
    TaskAttemptContext context = mock(TaskAttemptContext.class);
    when(context.getCounter(anyString(), anyString()))
        .thenReturn(mock(Counter.class));

    WarcRecordWriter recordWriter = new WarcRecordWriter(conf, outputPath, 0,
        context);
    recordWriter.write(new Text(requestedUrl),
        new WarcCapture(new Text(requestedUrl), datum, content));
    recordWriter.close(context);

    File[] warcFiles = new File(outputPath.toString(), "warc")
        .listFiles((dir, name) -> name.endsWith(".warc.gz"));
    assertNotNull(warcFiles, "No WARC file written");
    assertEquals(1, warcFiles.length, "Expected exactly one WARC file");

    // WarcWriter gzip-compresses every record, so the file must be inflated.
    String warcOutput;
    try (GZIPInputStream gis = new GZIPInputStream(
        new FileInputStream(warcFiles[0]))) {
      warcOutput = new String(gis.readAllBytes(), StandardCharsets.UTF_8);
    }

    assertTrue(warcOutput.contains("WARC-Target-URI: " + effectiveUrl),
        "WARC-Target-URI must be the effective (base) URL");
    assertFalse(warcOutput.contains("WARC-Target-URI: " + requestedUrl),
        "The malformed requested URL must not be used as WARC-Target-URI");
  }

  @Test
  public void testSelectTargetUrlPrefersEffectiveBaseUrl() {
    // Content.getUrl() stays the fetch key (keeps the parse/index join stable),
    // while Content.getBaseUrl() carries the effective URL the protocol put on
    // the wire. WARC-Target-URI must surface the effective URL.
    String fetchKey = "https://XN--e1afmkfd.example/A%2fb";
    String effectiveUrl = "https://xn--e1afmkfd.example/a/b";
    assertEquals(effectiveUrl,
        WarcRecordWriter.selectTargetUrl(fetchKey, effectiveUrl),
        "WARC-Target-URI must use the effective (base) URL when it differs from the fetch key");
  }

  @Test
  public void testSelectTargetUrlFallsBackToFetchKey() {
    // No effective URL available (e.g. protocols/records that don't set a
    // distinct base): the fetch key must be used so behaviour is unchanged.
    String fetchKey = "https://example.org/page";
    assertEquals(fetchKey, WarcRecordWriter.selectTargetUrl(fetchKey, null),
        "Null base URL must fall back to the fetch key");
    assertEquals(fetchKey, WarcRecordWriter.selectTargetUrl(fetchKey, ""),
        "Empty base URL must fall back to the fetch key");
  }
}
