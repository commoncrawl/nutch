package org.commoncrawl.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.commoncrawl.util.CombineSequenceFileInputFormat;
import org.commoncrawl.util.CompressedNutchWritable;
import org.commoncrawl.warc.WarcCompleteData;
import org.commoncrawl.warc.WarcOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class WarcExport extends Configured implements Tool {
  public static Logger LOG = LoggerFactory.getLogger(WarcExport.class);

  static {
    Configuration.addDefaultResource("nutch-default.xml");
    Configuration.addDefaultResource("nutch-site.xml");
  }

  public static class ExportMap extends Mapper<Text, Writable, Text, CompressedNutchWritable> {
    public void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
      if (key.getLength() == 0) {
        return;
      }
      context.write(key, new CompressedNutchWritable(value));
    }
  }


  public static class ExportReduce extends Reducer<Text, CompressedNutchWritable, Text, WarcCompleteData> {

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

      WarcCompleteData completeData = new WarcCompleteData(key, datum, content);

      context.write(key, completeData);
    }
  }

  public static class ParseDataCombinedInputFormat extends CombineSequenceFileInputFormat<Text, ParseData> {
  }

  public static class CrawlDatumCombinedInputFormat extends CombineSequenceFileInputFormat<Text, CrawlDatum> {
  }

  public void export(Path outputDir, List<Path> segments,
      boolean generateCrawlDiagnostics, boolean generateRobotsTxt, Path cdxPath)
      throws IOException {
    Configuration conf = getConf();

    // We compress ourselves, so this isn't necessary
    conf.setBoolean(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS, false);

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

    for (final Path segment : segments) {
      LOG.info("ExporterMapReduces: adding segment: " + segment);
      FileSystem fs = segment.getFileSystem(getConf());

      MultipleInputs.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME), CrawlDatumCombinedInputFormat.class);

      Path parseDataPath = new Path(segment, ParseData.DIR_NAME);
      if (fs.exists(parseDataPath)) {
        MultipleInputs.addInputPath(job, parseDataPath, ParseDataCombinedInputFormat.class);
      }

      MultipleInputs.addInputPath(job, new Path(segment, Content.DIR_NAME), ContentCombinedInputFormat.class);
    }

    job.setMapperClass(ExportMap.class);
    job.setReducerClass(ExportReduce.class);


    job.setMapOutputValueClass(CompressedNutchWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(WarcCompleteData.class);

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
      export(outputDir, segments, generateCrawlDiagnostics, generateRobotsTxt,
          cdxPath);
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
