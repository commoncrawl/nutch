package org.commoncrawl.tools;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.commoncrawl.warc.WarcWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class WarcExport extends Configured implements Tool {
  public static Logger LOG = LoggerFactory.getLogger(WarcExport.class);

  public static class CompleteData implements Writable {
    public Text url;
    public CrawlDatum datum;
    public Content content;
    public ParseText parseText;
    public ParseData parseData;

    public CompleteData() {
      url = new Text();
      datum = new CrawlDatum();
      content = new Content();
      parseText = new ParseText();
      parseData = new ParseData();
    }

    public CompleteData(Text url, CrawlDatum datum, Content content, ParseText parseText, ParseData parseData) {
      this.url = url;
      this.datum = datum;
      this.content = content;
      this.parseText = parseText;
      this.parseData = parseData;
    }

    public void readFields(DataInput in) throws IOException {
      url.readFields(in);
      datum.readFields(in);
      content.readFields(in);
      parseText.readFields(in);
      parseData.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      url.write(out);
      datum.write(out);
      content.write(out);
      parseText.write(out);
      parseData.write(out);
    }

    public String toString() {
      return "url=" + url.toString() + ", datum=" + datum.toString();
    }
  }

  public static class WarcOutputFormat extends FileOutputFormat<Text, CompleteData> {
    protected static class WarcRecordWriter implements RecordWriter<Text, CompleteData> {
      private DataOutputStream out;
      private WarcWriter writer;
      private URI warcinfoId;
      private final String CRLF = "\r\n";

      public WarcRecordWriter(DataOutputStream out, String filename) {
        this.out = out;

        writer = new WarcWriter(out);
        try {
          warcinfoId = writer.writeWarcinfoRecord(filename);
          System.out.println("Writing warcinfo record");
          System.out.println("Record ID: " + warcinfoId.toString());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public synchronized void write(Text key, CompleteData value) throws IOException {
        URI targetUri;

        try {
          targetUri = new URI(value.url.toString());
        } catch (URISyntaxException e) {
          return;
        }

        String ip = null;
        Date date = null;
        boolean notModified = false;
        String verbatimResponseHeaders;
        String verbatimRequestHeaders = null;
        String headers = new String();

        date = new Date(value.datum.getFetchTime());

        // Change this eventually
        boolean useVerbatimResponseHeaders = false;

        for (String name: value.parseData.getContentMeta().names()) {
          if (name.equals(Nutch.FETCH_DEST_IP_KEY)) {
            ip = value.parseData.getContentMeta().get(name);
          } else if (name.equals(Nutch.SEGMENT_NAME_KEY)) {
          } else if (name.equals(Nutch.FETCH_STATUS_KEY)) {
          } else if (name.equals(Nutch.SCORE_KEY)) {
          } else if (name.equals(Nutch.FETCH_REQUEST_VERBATIM_KEY)) {
            verbatimRequestHeaders = value.parseData.getContentMeta().get(name);
          } else if (name.equals(Nutch.CRAWL_DELAY_KEY)) {
          } else if (name.equals(Nutch.SIGNATURE_KEY)) {
          } else if (name.equals(Nutch.FETCH_RESPONSE_VERBATIM_HEADERS_KEY)) {
            verbatimResponseHeaders = value.parseData.getContentMeta().get(name);
          } else {
            // We have to fix up a few headers because we don't have the raw responses
            if (name.equalsIgnoreCase("Content-Length")) {
              headers += "Content-Length: " + value.content.getContent().length + CRLF;
            } else if (name.equalsIgnoreCase("Content-Encoding")) {
            } else if (name.equalsIgnoreCase("Transfer-Encoding")) {
            } else {
              headers += name + ": " + value.parseData.getContentMeta().get(name) + CRLF;
            }
          }
        }
        headers += CRLF;

        System.out.println("content.getMetadata()");
        System.out.println(value.content.getMetadata().toString());
        System.out.println(value.content.getMetadata().get(Response.CONTENT_LENGTH) + " vs " + value.content.getContent().length);


        System.out.println("headers");
        System.out.println("===========");
        System.out.print(headers);

        if (ip == null) {
          System.out.println("IP is null, returning");
          return;
        }

        if (verbatimRequestHeaders == null) {
          System.out.println("No request headers!");
          return;
        }

        System.out.println("\nIP: " + ip);

        StringBuilder requestsb = new StringBuilder(4096);
        requestsb.append(verbatimRequestHeaders);

        System.out.println("Request headers:");
        System.out.println("=================");
        System.out.println(requestsb.toString());

        byte[] requestBytes = requestsb.toString().getBytes("utf-8");
        InputStream request = new ByteArrayInputStream(requestBytes);

        System.out.println("About to write request record");
        URI requestId = writer.writeWarcRequestRecord(targetUri, ip, date, warcinfoId, request, requestBytes.length);

        if (notModified) {
          String payloadDigest;
          InputStream abbreviatedResponse;
          int abbreviatedResponseLength;

          /*
          writer.writeWarcRevisitRecord(targetUri, ip, date, warcinfoId, requestId,
              WarcWriter.PROFILE_REVISIT_NOT_MODIFIED, payloadDigest, abbreviatedResponse, abbreviatedResponseLength);
              */
        } else {
          StringBuilder responsesb = new StringBuilder(4096);
          responsesb.append("HTTP/1.0 200 OK").append(CRLF);
          responsesb.append(headers).append(CRLF);

          byte[] responseHeaderBytes = responsesb.toString().getBytes("utf-8");
          byte[] responseBytes = new byte[responseHeaderBytes.length + value.content.getContent().length];
          System.arraycopy(responseHeaderBytes, 0, responseBytes, 0, responseHeaderBytes.length);
          System.arraycopy(value.content.getContent(), 0, responseBytes, responseHeaderBytes.length,
              value.content.getContent().length);

          InputStream response = new ByteArrayInputStream(responseBytes);

          writer.writeWarcResponseRecord(targetUri, ip, date, warcinfoId, requestId, null, response, responseBytes.length);
        }
      }

      public synchronized void close(Reporter reporter) throws IOException {
        out.close();
      }
    }

    public RecordWriter<Text, CompleteData> getRecordWriter(FileSystem fs, JobConf job, String name,
                                                         Progressable progress) throws IOException {
      Path dir = FileOutputFormat.getOutputPath(job);
      String filename = name + ".warc.gz";
      DataOutputStream fileOut = fs.create(new Path(dir, filename), progress);
      return new WarcRecordWriter(fileOut, filename);
    }
  }

  public static class ExportMapReduce extends Configured
      implements Mapper<Text, Writable, Text, NutchWritable>,
      Reducer<Text, NutchWritable, Text, CompleteData> {

    public void configure(JobConf job) {
      setConf(job);
    }

    public void map(Text key, Writable value,
                    OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {

      String urlString = key.toString();
      if (urlString == null) {
        return;
      } else {
        key.set(urlString);
      }

      output.collect(key, new NutchWritable(value));
    }

    public void reduce(Text key, Iterator<NutchWritable> values,
                       OutputCollector<Text, CompleteData> output, Reporter reporter)
        throws IOException {
      Text url = key;
      CrawlDatum datum = null;
      Content content = null;
      ParseText parseText = null;
      ParseData parseData = null;

      while (values.hasNext()) {
        final Writable value = values.next().get(); // unwrap
        if (value instanceof CrawlDatum) {
          datum = (CrawlDatum)value;
        } else if (value instanceof ParseData) {
          parseData = (ParseData)value;
          // Get the robots meta data
          String robotsMeta = parseData.getMeta("robots");

          // Has it a noindex for this url?
          if (robotsMeta != null && robotsMeta.toLowerCase().indexOf("noindex") != -1) {
            return;
          }
        } else if (value instanceof ParseText) {
          parseText = (ParseText)value;
        } else if (value instanceof Content) {
          content = (Content)value;
        }
      }

      if (datum == null
          || parseData == null || parseText == null || content == null) {
        return;
      }

      if (datum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
        return;
      }

      CompleteData completeData = new CompleteData(url, datum, content, parseText, parseData);

      output.collect(url, completeData);

    }

    public void close() throws IOException { }
  }


  public void export(Path outputDir, List<Path> segments,
                     boolean filter, boolean normalize) throws IOException {

    System.out.println("In export!");
    final JobConf job = new NutchJob(getConf());
    job.setJobName("WarcExport");

    LOG.info("Exporter: URL filtering: " + filter);
    LOG.info("Exporter: URL normalizing: " + normalize);

    System.out.println("Adding segments!");
    for (final Path segment : segments) {
      LOG.info("ExporterMapReduces: adding segment: " + segment);
      FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
    }

    //FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(ExportMapReduce.class);
    job.setReducerClass(ExportMapReduce.class);

    System.out.println("Beep");

    job.setOutputFormat(WarcOutputFormat.class);
    job.setMapOutputValueClass(NutchWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CompleteData.class);

    System.out.println("Beep2");
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("Exporter: starting at " + sdf.format(start));

    // job.setBoolean(ExportMapReduce.URL_FILTERING, filter);
    // job.setBoolean(ExportMapReduce.URL_NORMALIZING, normalize);
    System.out.println("Beep3");
    job.setReduceSpeculativeExecution(false);
    FileOutputFormat.setOutputPath(job, outputDir);

    System.out.println("Trying to run job");
    try {
      JobClient.runJob(job);
      System.out.println("Wait what?");
      long end = System.currentTimeMillis();
      LOG.info("Exporter: finished at " + sdf.format(end) + ", elapsed: "
          + TimingUtil.elapsedTime(start, end));
    } finally {
      //FileSystem.get(job).delete(outputDir, true);
    }
  }


  public int run(String[] args) throws Exception {
    System.out.println("hello there");
    if (args.length < 2) {
      System.err
          .println("Usage: WarcExport <outputdir> (<segment> ... | -dir <segments>) [-noCommit] [-filter] [-normalize]");
      return -1;
    }

    final Path outputDir = new Path(args[0]);

    final List<Path> segments = new ArrayList<Path>();
    boolean filter = false;
    boolean normalize = false;

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
      } else if (args[i].equals("-filter")) {
        filter = true;
      } else if (args[i].equals("-normalize")) {
        normalize = true;
      } else {
        segments.add(new Path(args[i]));
      }
    }

    try {
      export(outputDir, segments, filter, normalize);
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
