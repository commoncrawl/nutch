package org.commoncrawl.warc;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.protocol.ProtocolStatus;
import org.commoncrawl.util.NullOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WarcOutputFormat extends FileOutputFormat<Text, WarcCompleteData> {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  private OutputCommitter committer;

  protected static class WarcRecordWriter extends RecordWriter<Text, WarcCompleteData> {
    private DataOutputStream warcOut;
    private WarcWriter warcWriter;
    private DataOutputStream crawlDiagnosticsWarcOut;
    private WarcWriter crawlDiagnosticsWarcWriter;
    private DataOutputStream robotsTxtWarcOut;
    private WarcWriter robotsTxtWarcWriter;
    private DataOutputStream cdxOut;
    private DataOutputStream crawlDiagnosticsCdxOut;
    private DataOutputStream robotsTxtCdxOut;
    private URI warcinfoId;
    private URI crawlDiagnosticsWarcinfoId;
    private URI robotsTxtWarcinfoId;
    private final String CRLF = "\r\n";
    private final String COLONSP = ": ";
    private MessageDigest sha1 = null;
    private Base32 base32 = null;
    private boolean generateCrawlDiagnostics;
    private boolean generateRobotsTxt;
    private boolean generateCdx;
    private String lastURL = ""; // for deduplication
    private boolean deduplicate;

    public WarcRecordWriter(Configuration conf, Path outputPath,
        String filename, String hostname, String publisher, String operator,
        String software, String isPartOf, String description,
        boolean generateCrawlDiagnostics, boolean generateRobotsTxt,
        boolean generateCdx, Path cdxPath, Date captureStartDate,
        boolean deduplicate) throws IOException {

      FileSystem fs = outputPath.getFileSystem(conf);

      Path warcPath = new Path(new Path(outputPath, "warc"), filename);
      warcOut = fs.create(warcPath);

      this.deduplicate = deduplicate;

      this.generateCdx = generateCdx;
      if (generateCdx) {
        cdxOut = openCdxOutputStream(new Path(cdxPath, "warc"), filename, conf);
        warcWriter = new WarcCdxWriter(warcOut, cdxOut, warcPath);
      } else {
        warcWriter = new WarcWriter(warcOut);
      }
      warcinfoId = warcWriter.writeWarcinfoRecord(filename, hostname, publisher,
          operator, software, isPartOf, description, captureStartDate);

      this.generateCrawlDiagnostics = generateCrawlDiagnostics;
      if (generateCrawlDiagnostics) {
        Path crawlDiagnosticsWarcPath = new Path(
            new Path(outputPath, "crawldiagnostics"), filename);
        crawlDiagnosticsWarcOut = fs.create(crawlDiagnosticsWarcPath);
        if (generateCdx) {
          crawlDiagnosticsCdxOut = openCdxOutputStream(new Path(cdxPath, "crawldiagnostics"),
              filename, conf);
          crawlDiagnosticsWarcWriter = new WarcCdxWriter(crawlDiagnosticsWarcOut,
              crawlDiagnosticsCdxOut, crawlDiagnosticsWarcPath);
        } else {
          crawlDiagnosticsWarcWriter = new WarcWriter(crawlDiagnosticsWarcOut);
        }
        crawlDiagnosticsWarcinfoId = crawlDiagnosticsWarcWriter
            .writeWarcinfoRecord(filename, hostname, publisher, operator,
                software, isPartOf, description, captureStartDate);
      }

      this.generateRobotsTxt = generateRobotsTxt;
      if (generateRobotsTxt) {
        Path robotsTxtWarcPath = new Path(new Path(outputPath, "robotstxt"),
            filename);
        robotsTxtWarcOut = fs.create(robotsTxtWarcPath);
        if (generateCdx) {
          robotsTxtCdxOut = openCdxOutputStream(new Path(cdxPath, "robotstxt"),
              filename, conf);
          robotsTxtWarcWriter = new WarcCdxWriter(robotsTxtWarcOut,
              robotsTxtCdxOut, robotsTxtWarcPath);
        } else {
          robotsTxtWarcWriter = new WarcWriter(robotsTxtWarcOut);
        }
        robotsTxtWarcinfoId = robotsTxtWarcWriter.writeWarcinfoRecord(
            filename, hostname, publisher, operator, software, isPartOf,
            description, captureStartDate);
      }

      base32 = new Base32();

      try {
        sha1 = MessageDigest.getInstance("SHA1");
      } catch (NoSuchAlgorithmException e) {
        LOG.error("Unable to instantiate SHA1 MessageDigest object");
        throw new RuntimeException(e);
      }
    }

    protected String getSha1DigestWithAlg(byte[] bytes) {
      sha1.reset();
      return "sha1:" + base32.encodeAsString(sha1.digest(bytes));
    }

    protected static int getStatusCode(String statusLine) {
      int start = statusLine.indexOf(" ");
      int end = statusLine.indexOf(" ", start + 1);
      if (end == -1)
        end = statusLine.length();
      int code = 200;
      try {
        code = Integer.parseInt(statusLine.substring(start + 1, end));
      } catch (NumberFormatException e) {
      }
      return code;
    }

    protected static DataOutputStream openCdxOutputStream(Path cdxPath,
        String warcFilename, Configuration conf) throws IOException {
      String cdxFilename = warcFilename.replaceFirst("\\.warc\\.gz$",
          ".cdx.gz");
      Path cdxFile = new Path(cdxPath, cdxFilename);
      FileSystem fs = cdxPath.getFileSystem(conf);
      return new DataOutputStream(new GZIPOutputStream(fs.create(cdxFile)));
    }

    public synchronized void write(Text key, WarcCompleteData value) throws IOException {
      URI targetUri;

      String url = value.url.toString();
      try {
        targetUri = new URI(url);
      } catch (URISyntaxException e) {
        LOG.error("Cannot write WARC record, invalid URI: {}", value.url);
        return;
      }

      if (value.content == null) {
        LOG.warn("Cannot write WARC record, no content for {}", value.url);
        return;
      }

      if (deduplicate) {
        if (lastURL.equals(url)) {
          LOG.info("Skipping duplicate record: {}", value.url);
          return;
        }
        lastURL = url;
      }

      String ip = "0.0.0.0";
      Date date = null;
      boolean notModified = false;
      String verbatimResponseHeaders = null;
      String verbatimRequestHeaders = null;
      String headers = "";
      String statusLine = "";
      int httpStatusCode = 200;
      String fetchDuration = null;
      String crawlDelay = null;

      if (value.datum != null) {
        date = new Date(value.datum.getFetchTime());
        // This is for older crawl dbs that don't include the verbatim status line in the metadata
        ProtocolStatus pstatus = (ProtocolStatus)value.datum.getMetaData().get(Nutch.WRITABLE_PROTO_STATUS_KEY);
        if (pstatus == null) {
          LOG.warn("Cannot write WARC record, no protocol status for {}",
              value.url);
          return;
        } else {
          switch (pstatus.getCode()) {
            case ProtocolStatus.SUCCESS:
              statusLine = "HTTP/1.0 200 OK";
              httpStatusCode = 200;
              break;
            case ProtocolStatus.TEMP_MOVED:
              statusLine = "HTTP/1.0 302 Found";
              httpStatusCode = 302;
              break;
            case ProtocolStatus.MOVED:
              statusLine = "HTTP/1.0 301 Moved Permanently";
              httpStatusCode = 301;
              break;
            case ProtocolStatus.NOTMODIFIED:
              statusLine = "HTTP/1.0 304 Not Modified";
              httpStatusCode = 304;
              notModified = true;
              break;
            default:
              if (value.content.getMetadata().get(Nutch.FETCH_RESPONSE_VERBATIM_STATUS_KEY) == null) {
                LOG.warn("Unknown or ambiguous protocol status: {}", pstatus);
                return;
              }
          }
        }
        if (value.datum.getMetaData().get(Nutch.WRITABLE_FETCH_DURATION_KEY) != null) {
          fetchDuration = value.datum.getMetaData().get(Nutch.WRITABLE_FETCH_DURATION_KEY).toString();
        }
        if (value.datum.getMetaData().get(Nutch.WRITABLE_CRAWL_DELAY_KEY) != null) {
          crawlDelay = value.datum.getMetaData().get(Nutch.WRITABLE_CRAWL_DELAY_KEY).toString();
        }
      } else {
        // robots.txt, no CrawlDatum available
        String fetchTime = value.content.getMetadata().get(Nutch.FETCH_TIME_KEY);
        if (fetchTime != null) {
          try {
            date = new Date(new Long(fetchTime));
          } catch (NumberFormatException e) {
            LOG.error("Invalid fetch time '{}' in content metadata of {}",
                fetchTime, value.url.toString());
          }
        }
        if (date == null) {
          String httpDate = value.content.getMetadata().get("Date");
          if (httpDate != null) {
            try {
              date = HttpDateFormat.toDate(httpDate);
            } catch (ParseException e) {
              LOG.warn("Failed to parse HTTP Date {} for {}", httpDate,
                  targetUri);
              date = new Date();
            }
          } else {
            LOG.warn("No HTTP Date for {}", targetUri);
            date = new Date();
          }
        }
        // status is taken from header
      }

      boolean useVerbatimResponseHeaders = false;
      boolean wasZipped = false;
      String truncatedReason = null;

      for (String name : value.content.getMetadata().names()) {
        if (name.equals(Nutch.FETCH_DEST_IP_KEY)) {
          ip = value.content.getMetadata().get(name);
        } else if (name.equals(Nutch.SEGMENT_NAME_KEY)) {
        } else if (name.equals(Nutch.FETCH_STATUS_KEY)) {
        } else if (name.equals(Nutch.SCORE_KEY)) {
        } else if (name.equals(Nutch.FETCH_REQUEST_VERBATIM_KEY)) {
          verbatimRequestHeaders = value.content.getMetadata().get(name);
        } else if (name.equals(Nutch.CRAWL_DELAY_KEY)) {
          crawlDelay = value.content.getMetadata().get(name);
        } else if (name.equals(Nutch.SIGNATURE_KEY)) {
        } else if (name.equals(Nutch.FETCH_RESPONSE_TRUNCATED_KEY)) {
          truncatedReason = value.content.getMetadata().get(name);
        } else if (name.equals(Nutch.FETCH_RESPONSE_VERBATIM_HEADERS_KEY)) {
          verbatimResponseHeaders = value.content.getMetadata().get(name);
          if (verbatimResponseHeaders.contains(CRLF)) {
            useVerbatimResponseHeaders = true;
          }
        } else if (name.equals(Nutch.FETCH_RESPONSE_VERBATIM_STATUS_KEY)) {
          statusLine = value.content.getMetadata().get(name);
          httpStatusCode = getStatusCode(statusLine);
        } else if (name.equals(Nutch.FETCH_RESPONSE_STATUS_CODE_KEY)) {
        } else {
          // We have to fix up a few headers because we don't have the raw responses
          if (name.equalsIgnoreCase("Content-Length")) {
            headers += "Content-Length: " + value.content.getContent().length + CRLF;
          } else if (name.equalsIgnoreCase("Content-Encoding")) {
            // we know that the content length can't be trusted 
            String ce = value.content.getMetadata().get(name);
            if ("gzip".equals(ce) || "x-gzip".equals(ce) || "deflate".equals(ce)) {
              wasZipped = true;
            }
          } else if (name.equalsIgnoreCase("Transfer-Encoding")) {
          } else {
            headers += name + ": " + value.content.getMetadata().get(name) + CRLF;
          }
        }
      }

      if (verbatimRequestHeaders == null) {
        LOG.error("No request headers!");
      }

      if (useVerbatimResponseHeaders && verbatimResponseHeaders != null
          && !wasZipped) {
        headers = verbatimResponseHeaders;
      }

      WarcWriter writer = warcWriter;
      URI infoId = this.warcinfoId;
      if (value.datum == null) {
        // no CrawlDatum: must be a robots.txt
        if (!generateRobotsTxt)
          return;
        writer = robotsTxtWarcWriter;
        infoId = robotsTxtWarcinfoId;
      } else if (value.datum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
        if (!generateCrawlDiagnostics)
          return;
        writer = crawlDiagnosticsWarcWriter;
        infoId = crawlDiagnosticsWarcinfoId;
      }

      URI requestId = null;
      if (verbatimRequestHeaders != null) {
        LOG.warn("c {} {} {}", targetUri, infoId, writer);
        requestId = writer.writeWarcRequestRecord(targetUri, ip, date, infoId,
            verbatimRequestHeaders.getBytes(StandardCharsets.UTF_8));
      }

      if (notModified) {
        LOG.warn("Revisit records not supported: {}", key);
        /*
        writer.writeWarcRevisitRecord(targetUri, ip, date, infoId, requestId,
            WarcWriter.PROFILE_REVISIT_NOT_MODIFIED, payloadDigest, abbreviatedResponse, abbreviatedResponseLength);
            */
      } else {
        StringBuilder responsesb = new StringBuilder(4096);
        responsesb.append(statusLine).append(CRLF);
        responsesb.append(headers).append(CRLF);

        byte[] responseHeaderBytes = responsesb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] responseBytes = new byte[responseHeaderBytes.length + value.content.getContent().length];
        System.arraycopy(responseHeaderBytes, 0, responseBytes, 0, responseHeaderBytes.length);
        System.arraycopy(value.content.getContent(), 0, responseBytes, responseHeaderBytes.length,
            value.content.getContent().length);

        if (generateCdx) {
          value.content.getMetadata().add("HTTP-Status-Code",
              String.format("%d", httpStatusCode));
        }

        URI responseId = writer.writeWarcResponseRecord(targetUri, ip, date,
            infoId, requestId,
            getSha1DigestWithAlg(value.content.getContent()),
            getSha1DigestWithAlg(responseBytes), truncatedReason,
            responseBytes, value.content);

        // Write metadata record
        StringBuilder metadatasb = new StringBuilder(4096);
        Map<String, String> metadata = new LinkedHashMap<String, String>();

        if (fetchDuration != null) {
          metadata.put("fetchTimeMs", fetchDuration);
        }

        if (crawlDelay != null) {
          metadata.put("robotsCrawlDelay", crawlDelay);
        }

        if (metadata.size() > 0) {
          for (Map.Entry<String, String> entry : metadata.entrySet()) {
            metadatasb.append(entry.getKey()).append(COLONSP)
                .append(entry.getValue()).append(CRLF);
          }
          metadatasb.append(CRLF);

          writer.writeWarcMetadataRecord(targetUri, date, infoId,
              responseId, null, metadatasb.toString().getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
      warcOut.close();
      if (generateCrawlDiagnostics) {
        crawlDiagnosticsWarcOut.close();
      }
      if (generateRobotsTxt) {
        robotsTxtWarcOut.close();
      }
      if (generateCdx) {
        cdxOut.close();
        if (generateCrawlDiagnostics) {
          crawlDiagnosticsCdxOut.close();
        }
        if (generateRobotsTxt) {
          robotsTxtCdxOut.close();
        }
      }
    }
  }

  public RecordWriter<Text, WarcCompleteData> getRecordWriter(JobConf conf,
      Path outputPath) throws IOException {

    int partition = conf.getInt("mapreduce.task.partition", -1);

    return getRecordWriter(conf, outputPath, partition);
  }

  public RecordWriter<Text, WarcCompleteData> getRecordWriter(
      TaskAttemptContext context) throws IOException {

    TaskID taskid = context.getTaskAttemptID().getTaskID();
    int partition = taskid.getId();
    Configuration conf = context.getConfiguration();
    Path outputPath = getOutputPath(context);

    return getRecordWriter(conf, outputPath, partition);
  }

  public RecordWriter<Text, WarcCompleteData> getRecordWriter(
      Configuration conf, Path outputPath, int partition) throws IOException {

    LOG.info("Partition: " + partition);

    String warcOutputPath = conf.get("warc.export.path");
    if (warcOutputPath != null) {
      LOG.info("Writing WARC output to {} as configured by warc.export.path",
          warcOutputPath);
      outputPath = new Path(warcOutputPath);
    }
    
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);

    SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US);
    fileDate.setTimeZone(TimeZone.getTimeZone("GMT"));

    String prefix = conf.get("warc.export.prefix", "CC-CRAWL");
    String prefixDate = conf.get("warc.export.date", fileDate.format(new Date()));
    String endDate = conf.get("warc.export.date.end", prefixDate);
    Date captureStartDate = new Date();
    try {
      captureStartDate = fileDate.parse(prefixDate);
    } catch (ParseException e) {
      LOG.error("Failed to parse warc.export.date {}: {}", prefixDate,
          e.getMessage());
    }

    String hostname = conf.get("warc.export.hostname", getHostname());
    String publisher = conf.get("warc.export.publisher", null);
    String operator = conf.get("warc.export.operator", null);
    String software = conf.get("warc.export.software", null);
    String isPartOf = conf.get("warc.export.isPartOf", null);
    String description = conf.get("warc.export.description", null);
    boolean generateCrawlDiagnostics = conf.getBoolean("warc.export.crawldiagnostics", false);
    boolean generateRobotsTxt = conf.getBoolean("warc.export.robotstxt", false);
    boolean generateCdx= conf.getBoolean("warc.export.cdx", false);
    boolean deduplicate = conf.getBoolean("warc.deduplicate", false);

    // WARC recommends - Prefix-Timestamp-Serial-Crawlhost.warc.gz
    //   https://github.com/iipc/warc-specifications/blob/gh-pages/specifications/warc-format/warc-1.1/index.md#annex-b-informative-warc-file-size-and-name-recommendations
    // WARC-Date : "The timestamp shall represent the instant that data
    //      capture for record creation began."
    //   https://github.com/iipc/warc-specifications/blob/gh-pages/specifications/warc-format/warc-1.1/index.md#warc-date-mandatory
    String filename = prefix + "-" + prefixDate + "-" + endDate + "-"
        + numberFormat.format(partition) + ".warc.gz";

    Path cdxPath = null;
    if (generateCdx) {
      cdxPath = new Path(
          conf.get("warc.export.cdx.path", outputPath.toString()));
    }

    return new WarcRecordWriter(conf, outputPath, filename, hostname, publisher,
        operator, software, isPartOf, description, generateCrawlDiagnostics,
        generateRobotsTxt, generateCdx, cdxPath, captureStartDate, deduplicate);
  }

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws java.io.IOException {
    if (committer == null) {
      Path output = getOutputPath(context);

      if (output.getFileSystem(context.getConfiguration()) instanceof NativeS3FileSystem ||
          output.getFileSystem(context.getConfiguration()).getScheme().equals("s3a")) {
        committer = new NullOutputCommitter();
      } else {
        committer = super.getOutputCommitter(context);
      }
    }
    return committer;
  }

  @Override
  public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { outDir }, job.getConfiguration());
  }

  public String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to get hostname: {}", e.getMessage());
    }
    return "localhost";
  }

}