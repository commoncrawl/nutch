package org.commoncrawl.tools;

import org.apache.commons.codec.binary.Base32;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.commoncrawl.util.CombineSequenceFileInputFormat;
import org.commoncrawl.util.CompressedNutchWritable;
import org.commoncrawl.util.NullOutputCommitter;
import org.commoncrawl.warc.WarcCdxWriter;
import org.commoncrawl.warc.WarcWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.zip.GZIPOutputStream;

public class WarcExport extends Configured implements Tool {
  public static Logger LOG = LoggerFactory.getLogger(WarcExport.class);

  static {
    Configuration.addDefaultResource("nutch-default.xml");
    Configuration.addDefaultResource("nutch-site.xml");
  }

  public static class CompleteData implements Writable {
    public Text url;
    public CrawlDatum datum;
    public Content content;
    public ParseText parseText;

    public CompleteData(Text url, CrawlDatum datum, Content content, ParseText parseText) {
      this.url = url;
      this.datum = datum;
      this.content = content;
      if (parseText != null) {
        this.parseText = parseText;
      } else {
        this.parseText = new ParseText();
      }
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
      content.readFields(in);
      parseText.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
      content.write(out);
      parseText.write(out);
    }

    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString();
    }
  }

  public static class WarcOutputFormat extends FileOutputFormat<Text, CompleteData> {
    private OutputCommitter committer;

    protected static class WarcRecordWriter extends RecordWriter<Text, CompleteData> {
      private DataOutputStream warcOut;
      private WarcWriter warcWriter;
      private DataOutputStream textWarcOut;
      private WarcWriter textWarcWriter;
      private DataOutputStream crawlDiagnosticsWarcOut;
      private WarcWriter crawlDiagnosticsWarcWriter;
      private DataOutputStream robotsTxtWarcOut;
      private WarcWriter robotsTxtWarcWriter;
      private DataOutputStream cdxOut;
      private DataOutputStream crawlDiagnosticsCdxOut;
      private DataOutputStream robotsTxtCdxOut;
      private URI warcinfoId;
      private URI textWarcinfoId;
      private URI crawlDiagnosticsWarcinfoId;
      private URI robotsTxtWarcinfoId;
      private final String CRLF = "\r\n";
      private final String COLONSP = ": ";
      private MessageDigest sha1 = null;
      private Base32 base32 = null;
      private boolean generateText;
      private boolean generateCrawlDiagnostics;
      private boolean generateRobotsTxt;
      private boolean generateCdx;

      public WarcRecordWriter(TaskAttemptContext context, Path outputPath,
          String filename, String textFilename, String hostname,
          String publisher, String operator, String software, String isPartOf,
          String description, boolean generateText,
          boolean generateCrawlDiagnostics, boolean generateRobotsTxt,
          boolean generateCdx, Path cdxPath) throws IOException {

        FileSystem fs = outputPath.getFileSystem(context.getConfiguration());

        Path warcPath = new Path(new Path(outputPath, "warc"), filename);
        warcOut = fs.create(warcPath);

        this.generateCdx = generateCdx;
        if (generateCdx) {
          cdxOut = openCdxOutputStream(new Path(cdxPath, "warc"), filename,
              context);
          warcWriter = new WarcCdxWriter(warcOut, cdxOut, warcPath);
        } else {
          warcWriter = new WarcWriter(warcOut);
        }
        warcinfoId = warcWriter.writeWarcinfoRecord(filename, hostname, publisher, operator, software, isPartOf, description);

        this.generateText = generateText;
        if (generateText) {
          textWarcOut = fs.create(new Path(new Path(outputPath, "text"), textFilename));
          textWarcWriter = new WarcWriter(textWarcOut);
          textWarcinfoId = textWarcWriter.writeWarcinfoRecord(textFilename, hostname, publisher, operator, software, isPartOf, description);
        }

        this.generateCrawlDiagnostics = generateCrawlDiagnostics;
        if (generateCrawlDiagnostics) {
          Path crawlDiagnosticsWarcPath = new Path(
              new Path(outputPath, "crawldiagnostics"), filename);
          crawlDiagnosticsWarcOut = fs.create(crawlDiagnosticsWarcPath);
          if (generateCdx) {
            crawlDiagnosticsCdxOut = openCdxOutputStream(new Path(cdxPath, "crawldiagnostics"),
                filename, context);
            crawlDiagnosticsWarcWriter = new WarcCdxWriter(crawlDiagnosticsWarcOut,
                crawlDiagnosticsCdxOut, crawlDiagnosticsWarcPath);
          } else {
            crawlDiagnosticsWarcWriter = new WarcWriter(crawlDiagnosticsWarcOut);
          }
          crawlDiagnosticsWarcinfoId = crawlDiagnosticsWarcWriter.writeWarcinfoRecord(filename, hostname, publisher, operator, software, isPartOf, description);
        }

        this.generateRobotsTxt = generateRobotsTxt;
        if (generateRobotsTxt) {
          Path robotsTxtWarcPath = new Path(new Path(outputPath, "robotstxt"),
              filename);
          robotsTxtWarcOut = fs.create(robotsTxtWarcPath);
          if (generateCdx) {
            robotsTxtCdxOut = openCdxOutputStream(new Path(cdxPath, "robotstxt"),
                filename, context);
            robotsTxtWarcWriter = new WarcCdxWriter(robotsTxtWarcOut,
                robotsTxtCdxOut, robotsTxtWarcPath);
          } else {
            robotsTxtWarcWriter = new WarcWriter(robotsTxtWarcOut);
          }
          robotsTxtWarcinfoId = robotsTxtWarcWriter.writeWarcinfoRecord(filename, hostname, publisher, operator, software, isPartOf, description);
        }

        base32 = new Base32();

        try {
          sha1 = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException e) {
          LOG.info("Unable to instantiate SHA1 MessageDigest object");
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
          String warcFilename, TaskAttemptContext context) throws IOException {
        String cdxFilename = warcFilename.replaceFirst("\\.warc\\.gz$",
            ".cdx.gz");
        Path cdxFile = new Path(cdxPath, cdxFilename);
        FileSystem fs = cdxPath.getFileSystem(context.getConfiguration());
        return new DataOutputStream(new GZIPOutputStream(fs.create(cdxFile)));
      }

      public synchronized void write(Text key, CompleteData value) throws IOException {
        URI targetUri;

        try {
          targetUri = new URI(value.url.toString());
        } catch (URISyntaxException e) {
          return;
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
                  LOG.info("Unknown or ambiguous protocol status");
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
          LOG.info("No request headers!");
          return;
        }

        if (useVerbatimResponseHeaders && verbatimResponseHeaders != null && !wasZipped) {
          headers = verbatimResponseHeaders;
        }

        WarcWriter writer = warcWriter;
        URI infoId = this.warcinfoId;
        if (value.datum == null) {
          // no CrawlDatum: must be a robots.txt
          writer = robotsTxtWarcWriter;
          infoId = robotsTxtWarcinfoId;
        } else if (value.datum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
          writer = crawlDiagnosticsWarcWriter;
          infoId = crawlDiagnosticsWarcinfoId;
        }

        URI requestId = writer.writeWarcRequestRecord(targetUri, ip, date, infoId,
            verbatimRequestHeaders.getBytes("utf-8"));

        if (notModified) {
          /*
          writer.writeWarcRevisitRecord(targetUri, ip, date, infoId, requestId,
              WarcWriter.PROFILE_REVISIT_NOT_MODIFIED, payloadDigest, abbreviatedResponse, abbreviatedResponseLength);
              */
        } else {
          StringBuilder responsesb = new StringBuilder(4096);
          responsesb.append(statusLine).append(CRLF);
          responsesb.append(headers).append(CRLF);

          byte[] responseHeaderBytes = responsesb.toString().getBytes("utf-8");
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
              responseBytes, value.content.getMetadata());

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
                responseId, null, metadatasb.toString().getBytes("utf-8"));
          }

          // Write text extract
          if (generateText && value.parseText != null) {
            final String text = value.parseText.getText();
            if (text != null) {
              byte[] conversionBytes = value.parseText.getText().getBytes("utf-8");
              if (conversionBytes.length != 0) {
                textWarcWriter.writeWarcConversionRecord(targetUri, date, textWarcinfoId, responseId,
                    getSha1DigestWithAlg(conversionBytes), "text/plain", conversionBytes);
              }
            }
          }
        }
      }

      public synchronized void close(TaskAttemptContext context) throws IOException {
        warcOut.close();
        if (generateText) {
          textWarcOut.close();
        }
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

    public RecordWriter<Text, CompleteData> getRecordWriter(TaskAttemptContext context) throws IOException {
      TaskID taskid = context.getTaskAttemptID().getTaskID();
      int partition = taskid.getId();
      LOG.info("Partition: " + partition);


      NumberFormat numberFormat = NumberFormat.getInstance();
      numberFormat.setMinimumIntegerDigits(5);
      numberFormat.setGroupingUsed(false);

      SimpleDateFormat fileDate = new SimpleDateFormat("yyyyMMddHHmmss");
      fileDate.setTimeZone(TimeZone.getTimeZone("GMT"));

      Configuration conf = context.getConfiguration();

      String prefix = conf.get("warc.export.prefix", "CC-CRAWL");
      String textPrefix = conf.get("warc.export.textprefix", "CC-CRAWL-TEXT");
      String prefixDate = conf.get("warc.export.date", fileDate.format(new Date()));

      String hostname = conf.get("warc.export.hostname", "localhost");
      String publisher = conf.get("warc.export.publisher", null);
      String operator = conf.get("warc.export.operator", null);
      String software = conf.get("warc.export.software", null);
      String isPartOf = conf.get("warc.export.isPartOf", null);
      String description = conf.get("warc.export.description", null);
      boolean generateText = conf.getBoolean("warc.export.text", true);
      boolean generateCrawlDiagnostics = conf.getBoolean("warc.export.crawldiagnostics", false);
      boolean generateRobotsTxt = conf.getBoolean("warc.export.robotstxt", false);
      boolean generateCdx= conf.getBoolean("warc.export.cdx", false);

      // WARC recommends - Prefix-Timestamp-Serial-Crawlhost.warc.gz
      String filename = prefix + "-" + prefixDate + "-" + numberFormat.format(partition) + "-" +
          hostname + ".warc.gz";
      String textFilename = textPrefix + "-" + prefixDate + "-" + numberFormat.format(partition) + "-" +
          hostname + ".warc.gz";


      Path outputPath = getOutputPath(context);
      Path cdxPath = null;
      if (generateCdx) {
        cdxPath = new Path(
            conf.get("warc.export.cdx.path", outputPath.toString()));
      }

      return new WarcRecordWriter(context, outputPath, filename, textFilename,
          hostname, publisher, operator, software, isPartOf, description,
          generateText, generateCrawlDiagnostics, generateRobotsTxt,
          generateCdx, cdxPath);
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
  }

  public static class ExportMap extends Mapper<Text, Writable, Text, CompressedNutchWritable> {
    public void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
      if (key.getLength() == 0) {
        return;
      }
      context.write(key, new CompressedNutchWritable(value));
    }
  }


  public static class ExportReduce extends Reducer<Text, CompressedNutchWritable, Text, CompleteData> {

    private boolean generateCrawlDiagnostics = false;
    private boolean generateRobotsTxt = false;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      generateCrawlDiagnostics = conf.getBoolean("warc.export.crawldiagnostics", false);
      generateRobotsTxt = conf.getBoolean("warc.export.robotstxt", false);
    }

    public void reduce(Text key, Iterable<CompressedNutchWritable> values, Context context) throws IOException, InterruptedException {
      CrawlDatum datum = null;
      Content content = null;
      ParseText parseText = null;

      for (CompressedNutchWritable nutchValue : values) {
        final Writable value = nutchValue.get(); // unwrap
        if (value instanceof CrawlDatum) {
          datum = (CrawlDatum)value;
        } else if (value instanceof ParseData) {
          ParseData parseData = (ParseData)value;
          // Get the robots meta data
          String robotsMeta = parseData.getMeta("robots");

          // Has it a noindex for this url?
          if (robotsMeta != null && robotsMeta.toLowerCase().contains("noindex")) {
            return;
          }
        } else if (value instanceof ParseText) {
          parseText = (ParseText)value;
        } else if (value instanceof Content) {
          content = (Content)value;
        }
      }

      if (content == null) {
        return;
      }

      if (datum == null) {
        if (!generateRobotsTxt)
          return;
      } else if (datum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
        if (!generateCrawlDiagnostics)
          return;
      }

      CompleteData completeData = new CompleteData(key, datum, content, parseText);

      context.write(key, completeData);
    }
  }

  public static class ParseDataCombinedInputFormat extends CombineSequenceFileInputFormat<Text, ParseData> {
  }

  public static class ParseTextCombinedInputFormat extends CombineSequenceFileInputFormat<Text, ParseText> {
  }

  public static class CrawlDatumCombinedInputFormat extends CombineSequenceFileInputFormat<Text, CrawlDatum> {
  }

  public String getHostname() {
    try {
      Process p = Runtime.getRuntime().exec("hostname -f");
      p.waitFor();
      BufferedReader in = new BufferedReader(
          new InputStreamReader(p.getInputStream()));
      String hostname = in.readLine();
      if (hostname != null && hostname.length() != 0 && p.exitValue() == 0) {
        return hostname;
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    return "localhost";
  }

  public void export(Path outputDir, List<Path> segments, boolean generateText,
      boolean generateCrawlDiagnostics, boolean generateRobotsTxt,
      Path cdxPath) throws IOException {
    Configuration conf = getConf();

    // We compress ourselves, so this isn't necessary
    conf.setBoolean(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS, false);
    conf.set("warc.export.hostname", getHostname());

    conf.setBoolean("warc.export.text", generateText);
    conf.setBoolean("warc.export.crawldiagnostics", generateCrawlDiagnostics);
    conf.setBoolean("warc.export.robotstxt", generateRobotsTxt);
    if (cdxPath != null) {
      conf.setBoolean("warc.export.cdx", true);
      conf.set("warc.export.cdx.path", cdxPath.toString());
    }

    Job job = Job.getInstance(conf);
    job.setJobName("WarcExport: " + outputDir.toString());
    job.setJarByClass(WarcExport.class);

    FileOutputFormat.setOutputPath(job, new Path("out"));

    LOG.info("Exporter: Text generation: " + generateText);

    for (final Path segment : segments) {
      LOG.info("ExporterMapReduces: adding segment: " + segment);
      FileSystem fs = segment.getFileSystem(getConf());

      MultipleInputs.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME), CrawlDatumCombinedInputFormat.class);

      Path parseDataPath = new Path(segment, ParseData.DIR_NAME);
      if (fs.exists(parseDataPath)) {
        MultipleInputs.addInputPath(job, parseDataPath, ParseDataCombinedInputFormat.class);
      }

      Path parseTextPath = new Path(segment, ParseText.DIR_NAME);
      if (generateText) {
        if (fs.exists(parseTextPath)) {
          MultipleInputs.addInputPath(job, parseTextPath, ParseTextCombinedInputFormat.class);
        } else {
          LOG.warn("ParseText path doesn't exist: " + parseTextPath.toString());
        }
      }

      MultipleInputs.addInputPath(job, new Path(segment, Content.DIR_NAME), ContentCombinedInputFormat.class);
    }

    job.setMapperClass(ExportMap.class);
    job.setReducerClass(ExportReduce.class);


    job.setMapOutputValueClass(CompressedNutchWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CompleteData.class);

    job.setOutputFormatClass(WarcOutputFormat.class);
    WarcOutputFormat.setOutputPath(job, outputDir);


    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Exporter: starting at " + sdf.format(start));

    try {
      boolean result = job.waitForCompletion(true);
      LOG.info("Return from waitForCompletion: " + result);
    } catch (Exception e) {
      LOG.error("Caught exception while trying to run job", e);
    }
  }


  public static class ContentCombinedInputFormat extends CombineSequenceFileInputFormat<Text, Content> {
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err
          .println("Usage: WarcExport <outputdir> (<segment> ... | -dir <segments>) [-notext] [-crawldiagnostics] [-robotstxt] [-cdx path]");
      return -1;
    }

    final Path outputDir = new Path(args[0]);

    final List<Path> segments = new ArrayList<Path>();
    boolean generateText = true;
    boolean generateCrawlDiagnostics = false;
    boolean generateRobotsTxt = false;
    Path cdxPath = null;

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-dir")) {
        Path dir = new Path(args[++i]);
        FileSystem fs = dir.getFileSystem(getConf());
        FileStatus[] fstats = fs.listStatus(dir,
            HadoopFSUtil.getPassDirectoriesFilter(fs));
        Path[] files = HadoopFSUtil.getPaths(fstats);
        for (Path p : files) {
          segments.add(p);
        }
      } else if (args[i].equals("-notext")) {
        generateText = false;
      } else if (args[i].equals("-crawldiagnostics")) {
        generateCrawlDiagnostics = true;
      } else if (args[i].equals("-robotstxt")) {
        generateRobotsTxt = true;
      } else if (args[i].equals("-cdx")) {
        cdxPath = new Path(args[++i]);
      } else {
        segments.add(new Path(args[i]));
      }
    }

    try {
      export(outputDir, segments, generateText, generateCrawlDiagnostics,
          generateRobotsTxt, cdxPath);
      return 0;
    } catch (final Exception e) {
      LOG.error("Exporter: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  public static void main(String[] args) throws Exception {
    final int res = ToolRunner.run(NutchConfiguration.create(),
        new WarcExport(), args);
    System.exit(res);
  }
}
