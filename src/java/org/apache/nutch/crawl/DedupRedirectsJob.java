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
package org.apache.nutch.crawl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple deduplication of redirects. If two redirects point to the same target
 * <ul>
 * <li>... and the target is in CrawlDb: both redirects are marked as
 * duplicates</li>
 * <li>... and the target is not in CrawlDb: one redirect is chosen depending on
 * the criteria defined by -compareOrder and marked as duplicate</li>
 * </ul>
 *
 * Unlike {@link DeduplicationJob} which deduplicates based on content-based
 * signatures, deduplication of redirects is not done to remove clean-up the
 * index (redirects are not indexed). The aim to mark URLs which would cause
 * duplicate content when the fetcher is following redirects
 * (http.redirects.max&nbsp;&gt;&nbsp;0).
 */
public class DedupRedirectsJob extends DeduplicationJob {

  public static final Logger LOG = LoggerFactory
      .getLogger(DedupRedirectsJob.class);

  /**
   * Test whether CrawlDatum is a redirect.
   *
   * @param datum
   * @return true if datum is a redirect, false otherwise
   */
  public static boolean isRedirect(CrawlDatum datum) {
    byte status = datum.getStatus();
    if (status == CrawlDatum.STATUS_DB_REDIR_PERM
        || status == CrawlDatum.STATUS_DB_REDIR_TEMP) {
      return true;
    }
    if (status == CrawlDatum.STATUS_DB_DUPLICATE) {
      ProtocolStatus pStatus = getProtocolStatus(datum);
      if (pStatus != null && (pStatus.getCode() == ProtocolStatus.MOVED
          || pStatus.getCode() == ProtocolStatus.TEMP_MOVED)) {
        // redirect already marked as duplicate
        return true;
      }
    }
    return false;
  }

  /**
   * Return protocol status of CrawlDatum if present.
   * 
   * @param datum
   * @return protocol status or null if not present
   */
  public static ProtocolStatus getProtocolStatus(CrawlDatum datum) {
    if (datum.getMetaData().containsKey(Nutch.WRITABLE_PROTO_STATUS_KEY))
      return (ProtocolStatus) datum.getMetaData()
          .get(Nutch.WRITABLE_PROTO_STATUS_KEY);
    return null;
  }

  /**
   * Get target URL of a redirect. Note: CrawlDatum is assumed to be a redirect.
   *
   * @param datum
   * @return redirect target URL or null if not available or datum is not a
   *         redirect
   */
  public static String getTargetURL(CrawlDatum datum) {
    ProtocolStatus pStatus = getProtocolStatus(datum);
    if (pStatus != null) {
      return pStatus.getMessage();
    }
    return null;
  }

  /**
   * Reset duplicate status of a redirect marked as duplicate and restore old
   * status (permanent or temporary redirect).
   *
   * @param datum
   */
  private static void unsetDuplicateStatus(CrawlDatum datum) {
    byte status = datum.getStatus();
    if (status == CrawlDatum.STATUS_DB_DUPLICATE) {
      ProtocolStatus pStatus =  getProtocolStatus(datum);
      if (pStatus != null) {
        int code = pStatus.getCode();
        if (code == ProtocolStatus.MOVED)
          datum.setStatus(CrawlDatum.STATUS_DB_REDIR_PERM);
        else if (code == ProtocolStatus.TEMP_MOVED)
          datum.setStatus(CrawlDatum.STATUS_DB_REDIR_TEMP);
      }
    }
  }

  public static class RedirTargetMapper
      implements Mapper<Text, CrawlDatum, Text, CrawlDatum> {

    @Override
    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      // output independent of status
      output.collect(key, value);

      if (isRedirect(value)) {
        String redirTarget = getTargetURL(value);
        if (redirTarget != null) {
          // keep original URL in CrawlDatum's meta data and emit
          // <redirTarget, crawlDatum>
          value.getMetaData().put(urlKey, key);
          Text redirKey = new Text(redirTarget);
          reporter.incrCounter("DeduplicationJobStatus",
              "Redirects in CrawlDb", 1);
          if (redirKey.equals(key)) {
            // exclude self-referential redirects
            reporter.incrCounter("DeduplicationJobStatus",
                "Self-referential redirects in CrawlDb", 1);
          } else {
            output.collect(redirKey, value);
          }
        }
      }
    }

    @Override
    public void configure(JobConf conf) {
    }

    @Override
    public void close() throws IOException {
    }

  }

  public static class DedupReducer extends DeduplicationJob.DedupReducer<Text> {

    @Override
    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
      CrawlDatum existingDoc = null;
      while (values.hasNext()) {
        if (existingDoc == null) {
          existingDoc = new CrawlDatum();
          existingDoc.set(values.next());
          continue;
        }
        CrawlDatum newDoc = values.next();
        CrawlDatum duplicate = null;
        if (isRedirect(existingDoc)
            && !newDoc.getMetaData().containsKey(urlKey)) {
          // newDoc is known as redirect target
          writeOutAsDuplicate(existingDoc, output, reporter);
          existingDoc.set(newDoc);
        } else if (isRedirect(newDoc)
            && !existingDoc.getMetaData().containsKey(urlKey)) {
          // existingDoc is known as redirect target
          writeOutAsDuplicate(newDoc, output, reporter);
        } else {
          // existingDoc and newDoc are redirects and point to the same target
          duplicate = getDuplicate(existingDoc, newDoc);
          if (duplicate == null) {
            // no decision possible in getDuplicate()
            // and both are redirects: dedup newDoc 
            duplicate = newDoc;
          }
          writeOutAsDuplicate(duplicate, output, reporter);
          if (duplicate == existingDoc) {
            existingDoc.set(newDoc);
          }
        }
      }
      // finally output existingDoc as non-duplicate if
      if (!isRedirect(existingDoc)) {
        // (a) it is not a redirect
        output.collect(key, existingDoc);
      } else {
        // (b) it is not the value passed to the reducer under original URL key
        Text origURL = (Text) existingDoc.getMetaData().remove(urlKey);
        if (origURL != null) {
          unsetDuplicateStatus(existingDoc);
          output.collect(key, existingDoc);
          reporter.incrCounter("DeduplicationJobStatus",
              "Redirects kept as non-duplicates", 1);
        } else {
          // (c) it is a self-referential redirect
          String targetURL = getTargetURL(existingDoc);
          if (key.toString().equals(targetURL)) {
            reporter.incrCounter("DeduplicationJobStatus",
                "Self-referential redirects kept as non-duplicates", 1);            
          }
          // else: ignore redirects emitted under original URL
        }
      }
    }
  }

  public int run(String[] args) throws IOException {

    if (args.length < 1) {
      System.err.println(
          "Usage: DedupRedirJob <crawldb> [-group <none|host|domain>] [-compareOrder <score>,<fetchTime>,<urlLength>]");
      return 1;
    }

    String group = "none";
    String crawldb = args[0];
    String compareOrder = "score,fetchTime,urlLength";

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("-group"))
        group = args[++i];
      if (args[i].equals("-compareOrder")) {
        compareOrder = args[++i];

        if (compareOrder.indexOf("score") == -1
            || compareOrder.indexOf("fetchTime") == -1
            || compareOrder.indexOf("urlLength") == -1) {
          System.err.println(
              "DedupRedirJob: compareOrder must contain score, fetchTime and urlLength.");
          return 1;
        }
      }
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    LOG.info("DedupRedirectJob: starting at " + sdf.format(start));

    String dedupTemp = "dedup-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
    Path tempDir = new Path(
        getConf().get("mapred.temp.dir", "."), dedupTemp);

    JobConf job = new NutchJob(getConf());

    job.setJobName("Redirect deduplication on " + crawldb);
    job.set(DEDUPLICATION_GROUP_MODE, group);
    job.set(DEDUPLICATION_COMPARE_ORDER, compareOrder);

    FileInputFormat.addInputPath(job, new Path(crawldb, CrawlDb.CURRENT_NAME));
    job.setInputFormat(SequenceFileInputFormat.class);

    FileOutputFormat.setOutputPath(job, tempDir);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(CrawlDatum.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CrawlDatum.class);

    job.setMapperClass(RedirTargetMapper.class);
    job.setReducerClass(DedupReducer.class);

    try {
      RunningJob rj = JobClient.runJob(job);
      Group g = rj.getCounters().getGroup("DeduplicationJobStatus");
      if (g != null) {
        long dups = g.getCounter("Documents marked as duplicate");
        LOG.info("Deduplication: " + (int) dups
            + " documents marked as duplicates");
      }
    } catch (final Exception e) {
      LOG.error("DedupRedirJob: " + StringUtils.stringifyException(e));
      return -1;
    }

    // merge with existing crawl db
    if (LOG.isInfoEnabled()) {
      LOG.info("Redirect deduplication: writing CrawlDb.");
    }

    Path dbPath = new Path(crawldb);
    Path newDbPath = new Path(crawldb, dedupTemp);
    JobConf mergeJob = CrawlDb.createJob(getConf(), newDbPath);
    FileInputFormat.addInputPath(mergeJob, tempDir);
    mergeJob.setReducerClass(StatusUpdateReducer.class);

    try {
      JobClient.runJob(mergeJob);
    } catch (final Exception e) {
      LOG.error("DedupRedirJob: " + StringUtils.stringifyException(e));
      return -1;
    }

    CrawlDb.install(mergeJob, dbPath);
    newDbPath.getFileSystem(getConf()).delete(newDbPath, true);

    // clean up
    FileSystem fs = FileSystem.get(getConf());
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("DedupRedirJob finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(NutchConfiguration.create(),
        new DedupRedirectsJob(), args);
    System.exit(result);
  }

}
