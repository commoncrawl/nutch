/**
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
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;


/**
 * Inject URLs from sitemaps (http://www.sitemaps.org/).
 *
 * Sitemap URLs are given same way as "ordinary" seeds URLs - one URL per line.
 * Each URL points to one of
 * <ul>
 * <li>XML sitemap</li>
 * <li>plain text sitemap (possibly compressed)</li>
 * <li>sitemap index (XML)</li>
 * <li>and all <a
 * href="http://www.sitemaps.org/protocol.html#otherformats">other formats</a>
 * supported by the Sitemap parser of <a
 * href="http://code.google.com/p/crawler-commons/">crawler-commons</a>.</li>
 * </ul>
 *
 * <p>
 * All sitemap URLs on the input path are fetched and the URLs contained in the
 * sitemaps are "injected" into CrawlDb. If a sitemap specifies modification
 * time, refresh rate, and/or priority for a page, these values are stored in
 * CrawlDb but adjusted so that they fit into global limits. E.g.,
 *
 * <pre>
 * &lt;changefreq&gt;yearly&lt;/changefreq&gt;
 * </pre>
 *
 * may be limited to the value of property
 * <code>db.fetch.schedule.max_interval</code> and/or
 * <code>db.fetch.interval.max</code>.
 * </p>
 *
 * The typical use case for the SitemapInjector is to feed the crawl with a list
 * of URLs maintained by the site's owner (generated, e.g., via content
 * management system).
 *
 * Fetching sitemaps is done by Nutch protocol plugins to make use of special
 * settings, e.g., HTTP proxy configurations.
 *
 * The behavior how entries in CrawlDb are overwritten by injected entries does
 * not differ from {@link Injector}.
 *
 * <h2>Limitations</h2>
 *
 * <p>
 * SitemapInjector does not support:
 * <ul>
 * <li>follow redirects</li>
 * <li>no retry scheduling if fetching a sitemap fails</li>
 * <li>be polite and add delays between fetching sitemaps. Usually, there is
 * only one sitemap per host, so this does not matter that much.</li>
 * <li>check for &quot;<a
 * href="http://www.sitemaps.org/protocol.html#location">cross
 * submits</a>&quot;: if a sitemap URL is explicitly given it is assumed the
 * sitemap's content is trustworthy</li>
 * </ul>
 * </p>
 */
public class SitemapInjector extends Injector {

  /** Fetch and parse sitemaps, output extracted URLs as seeds */
  public static class SitemapInjectMapper extends InjectMapper {

    protected float minInterval;
    protected float maxInterval;

    protected int maxRecursiveSitemaps = 50001;
    protected long maxRecursiveUrlsPerSitemapIndex = 50000L * 50000;

    private ProtocolFactory protocolFactory;
    private SiteMapParser sitemapParser;

    private int maxSitemapFetchTime = 180;
    private ExecutorService executorService;
    
    public void configure(JobConf job) {
      super.configure(job);

      protocolFactory = new ProtocolFactory(job);

      // non-strict SiteMapParser (allow "cross submits")
      // TODO: make it configurable ?
      sitemapParser = new SiteMapParser(false);

      maxRecursiveSitemaps = jobConf.getInt("db.injector.sitemap.index_max_size", 50001);
      maxRecursiveUrlsPerSitemapIndex = jobConf.getLong("db.injector.sitemap.max_urls", 50000L * 50000);

      // fetch intervals defined in sitemap should within the defined range
      minInterval = jobConf.getFloat("db.fetch.schedule.adaptive.min_interval", 60);
      maxInterval = jobConf.getFloat("db.fetch.schedule.max_interval", 365*24*3600);
      if (maxInterval > jobConf.getInt("db.fetch.interval.max", 365*24*3600)) {
        maxInterval = jobConf.getInt("db.fetch.interval.max", 365*24*3600);
      }

      // Sitemaps can be quite large, so it is desirable to
      // increase the content limits above defaults (64kB):
      // TODO: make configurable?
      String[] contentLimitProperties = { "http.content.limit",
          "ftp.content.limit", "file.content.limit" };
      for (int i = 0; i < contentLimitProperties.length; i++) {
        jobConf.setInt(contentLimitProperties[i], SiteMapParser.MAX_BYTES_ALLOWED);
      }

      executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
          .setNameFormat("sitemapinj-%d").setDaemon(true).build());
    }

    public void map(WritableComparable<?> key, Text value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      // value is one line in the text file (= one sitemap URL)
      String url = value.toString();
      if (url != null && url.trim().startsWith("#")) {
        /* Ignore line that start with # */
        return;
      }

      Content content = getContent(url);
      if (content == null) {
        reporter
            .getCounter("SitemapInjector", "failed to fetch sitemap content")
            .increment(1);
        return;
      }

      AbstractSiteMap sitemap = null;
      try {
        sitemap = sitemapParser.parseSiteMap(content.getContentType(),
            content.getContent(), new URL(url));
      } catch (Exception e) {
        reporter.getCounter("SitemapInjector", "sitemaps failed to parse").increment(1);
        LOG.warn("failed to parse sitemap {}: {}", url,
            StringUtils.stringifyException(e));
        return;
      }

      LOG.info("parsed sitemap " + url + " (" + sitemap.getType() + ")");
      processSitemap(sitemap, output, reporter, new AtomicLong(0), null);
    }

    class FetchSitemapCallable implements Callable<ProtocolOutput> {
      private Protocol protocol;
      private String url;

      public FetchSitemapCallable(Protocol protocol, String url) {
        this.protocol = protocol;
        this.url = url;
      }

      @Override
      public ProtocolOutput call() throws Exception {
        return protocol.getProtocolOutput(new Text(url), new CrawlDatum());
      }
    }

    private Content getContent(String url) {
      Protocol protocol = null;
      try {
        protocol = protocolFactory.getProtocol(url);
      } catch (ProtocolNotFound e) {
        LOG.error("protocol not found " + url);
        return null;
      }

      LOG.info("fetching sitemap " + url);
      FetchSitemapCallable fetch = new FetchSitemapCallable(protocol, url);
      Future<ProtocolOutput> task = executorService.submit(fetch);
      ProtocolOutput protocolOutput = null;
      try {
        protocolOutput = task.get(maxSitemapFetchTime, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.error("fetch of sitemap {} failed with: {}", url,
            StringUtils.stringifyException(e));
        task.cancel(true);
        return null;
      } finally {
        fetch = null;
      }

      Content content = protocolOutput.getContent();
      if (content == null) {
        LOG.error("fetch of sitemap " + url + " failed with: "
            + protocolOutput.getStatus().getMessage());
        return null;
      }
      return content;
    }

    /**
     * parse a sitemap (recursively, in case of a sitemap index),
     * and inject all contained URLs
     * @param reporter
     */
    public void processSitemap(AbstractSiteMap sitemap,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter,
        AtomicLong totalUrls, Set<String> processedSitemaps) throws IOException {

      if (sitemap.isIndex()) {
        SiteMapIndex sitemapIndex = (SiteMapIndex) sitemap;
        if (processedSitemaps == null) {
          processedSitemaps = new HashSet<String>();
          processedSitemaps.add(sitemap.getUrl().toString());
        }

        while (sitemapIndex.hasUnprocessedSitemap()) {
          AbstractSiteMap nextSitemap = sitemapIndex.nextUnprocessedSitemap();
          reporter.getCounter("SitemapInjector", "sitemap indexes processed").increment(1);

          String url = nextSitemap.getUrl().toString();
          if (processedSitemaps.contains(url)) {
            LOG.warn("skipped recursive sitemap URL {}", url);
            reporter.getCounter("SitemapInjector", "skipped recursive sitemap URLs").increment(1);
            nextSitemap.setProcessed(true);
            continue;
          }
          if (processedSitemaps.size() > maxRecursiveSitemaps) {
            LOG.warn("{} sitemaps processed for {}, skipped remaining sitemaps",
                processedSitemaps.size(), sitemap.getUrl());
            reporter.getCounter("SitemapInjector", "sitemap index limit reached").increment(1);
            return;
          }
          if (totalUrls.longValue() > maxRecursiveUrlsPerSitemapIndex) {
            LOG.warn(
                "URL limit reached, skipped remaining sitemaps of sitemap index {}",
                sitemap.getUrl());
            reporter.getCounter("SitemapInjector", "sitemap index URL limit reached").increment(1);
            return;
          }

          processedSitemaps.add(url);

          Content content = getContent(url);
          if (content == null) {
            sitemapIndex.getSitemaps().remove(nextSitemap);
            nextSitemap.setProcessed(true);
            reporter.getCounter("SitemapInjector", "sitemaps failed to fetch").increment(1);
            return; // TODO: count failed sub-sitemaps, break loop if a limit is reached
          }

          try {
            AbstractSiteMap parsedSitemap = sitemapParser.parseSiteMap(
                content.getContentType(), content.getContent(), nextSitemap);
            processSitemap(parsedSitemap, output, reporter, totalUrls, processedSitemaps);
          } catch (Exception e) {
            LOG.warn("failed to parse sitemap {}: {}" + nextSitemap.getUrl(),
                StringUtils.stringifyException(e));
            reporter.getCounter("SitemapInjector", "sitemaps failed to parse").increment(1);
          }
          nextSitemap.setProcessed(true);
        }

      } else {
        reporter.getCounter("SitemapInjector", "sitemaps processed").increment(1);
        injectURLs((SiteMap) sitemap, output, reporter, totalUrls);
        if (totalUrls.longValue() > maxRecursiveUrlsPerSitemapIndex) {
          LOG.warn(
              "URL limit reached, skipped remaining urls of {}",
              sitemap.getUrl());
          reporter.getCounter("SitemapInjector", "sitemap index URL limit reached").increment(1);
          return;
        }
        sitemap.setProcessed(true);
      }
    }

    /**
     * Inject all URLs contained in one {@link SiteMap}.
     */
    public void injectURLs(SiteMap sitemap,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter,
        AtomicLong totalUrls) throws IOException {

      Collection<SiteMapURL> sitemapURLs = sitemap.getSiteMapUrls();
      LOG.info("injecting " + sitemapURLs.size() + " URLs from "
          + sitemap.getUrl());

      for (SiteMapURL siteMapURL : sitemapURLs) {

        if (totalUrls.longValue() > maxRecursiveUrlsPerSitemapIndex) {
          reporter.getCounter("SitemapInjector", "sitemap URL limit reached").increment(1);
          return;
        }
        totalUrls.incrementAndGet();

        // TODO: default priority should be 0.5 as stated
        //        in http://www.sitemaps.org/protocol.html#xmlTagDefinitions
        float customScore = (float) siteMapURL.getPriority();
        int customInterval = getChangeFrequencySeconds(siteMapURL.getChangeFrequency());
        long lastModified = -1;
        if (siteMapURL.getLastModified() != null) {
          lastModified = siteMapURL.getLastModified().getTime();
        }

        String url = siteMapURL.getUrl().toString();
        try {
          url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
          url = filters.filter(url);
        } catch (Exception e) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("Skipping {}: {}", url, StringUtils.stringifyException(e));
          }
          url = null;
        }
        if (url == null) {
          reporter.getCounter("SitemapInjector", "urls from sitemaps rejected by URL filters").increment(1);
        } else {
          // URL passed normalizers and filters
          Text value = new Text(url);
          CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_INJECTED,
              customInterval, customScore);
          if (lastModified != -1) {
            // datum.setModifiedTime(lastModified);
          }
          datum.setFetchTime(curTime);

          try {
            scfilters.injectedScore(value, datum);
          } catch (ScoringFilterException e) {
            if (LOG.isWarnEnabled()) {
              LOG.warn("Cannot filter injected score for url " + url
                  + ", using default (" + e.getMessage() + ")");
            }
          }

          reporter.getCounter("SitemapInjector", "urls from sitemaps injected").increment(1);
          output.collect(value, datum);
        }
      }
    }

    /**
     * Determine fetch schedule intervals based on given
     * <code>changefrequency</code> but adjusted to min. and max. intervals
     *
     * @param changeFrequency
     * @return interval in seconds
     */
    private int getChangeFrequencySeconds(
        SiteMapURL.ChangeFrequency changeFrequency) {
      float cf = interval;
      if (changeFrequency != null) {
        switch (changeFrequency) {
        case NEVER:
          cf = maxInterval;
          break;
        case YEARLY:
          cf = 365*24*3600;
          break;
        case MONTHLY:
          cf = 30*24*3600;
          break;
        case WEEKLY:
          cf = 7*24*3600;
          break;
        case DAILY:
          cf = 24*3600;
          break;
        case HOURLY:
          cf = 3600;
          break;
        case ALWAYS:
          cf = minInterval;
          break;
        }
      }
      if (cf < minInterval) {
        cf = minInterval;
      } else if(cf > maxInterval) {
        cf = maxInterval;
      }
       return (int) cf;
    }

  }

  public void inject(Path crawlDb, Path urlDir) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapInjector: starting at " + sdf.format(start));
      LOG.info("SitemapInjector: crawlDb: " + crawlDb);
      LOG.info("SitemapInjector: urlDir: " + urlDir);
    }

    Path tempDir =
      new Path(getConf().get("mapred.temp.dir", ".") +
          "/sitemap-inject-temp-"+
          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // for all sitemap URLs listed in text input file(s)
    // fetch and parse the sitemap, and map the contained URLs to <url,CrawlDatum> pairs
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapInjector: Converting injected urls to crawl db entries.");
    }
    JobConf sortJob = new NutchJob(getConf());
    sortJob.setJobName("inject " + urlDir);
    FileInputFormat.addInputPath(sortJob, urlDir);
    sortJob.setMapperClass(SitemapInjectMapper.class);

    FileOutputFormat.setOutputPath(sortJob, tempDir);
    sortJob.setOutputFormat(SequenceFileOutputFormat.class);
    sortJob.setOutputKeyClass(Text.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    sortJob.setMapSpeculativeExecution(false); // mappers are fetching sitemaps
    sortJob.setLong("injector.current.time", System.currentTimeMillis());
    RunningJob mapJob = JobClient.runJob(sortJob);

    for (Counter counter : mapJob.getCounters().getGroup("SitemapInjector")) {
      LOG.info(String.format("SitemapInjector: %8d  %s", counter.getValue(),
          counter.getName()));
    }

    // merge with existing crawl db
    if (LOG.isInfoEnabled()) {
      LOG.info("SitemapInjector: Merging injected urls into crawl db.");
    }
    JobConf mergeJob = CrawlDb.createJob(getConf(), crawlDb);
    FileInputFormat.addInputPath(mergeJob, tempDir);
    mergeJob.setReducerClass(InjectReducer.class);
    JobClient.runJob(mergeJob);
    CrawlDb.install(mergeJob, crawlDb);

    // clean up
    FileSystem fs = FileSystem.get(getConf());
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("SitemapInjector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new SitemapInjector(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: SitemapInjector <crawldb> <url_dir>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.error("SitemapInjector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }
}
