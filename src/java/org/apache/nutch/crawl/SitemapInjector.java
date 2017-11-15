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
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.lib.MultithreadedMapRunner;
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
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolNotFound;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import crawlercommons.robots.BaseRobotRules;
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

    private static final String SITEMAP_MAX_URLS = "db.injector.sitemap.max_urls";

    protected float minInterval;
    protected float maxInterval;

    protected int maxRecursiveSitemaps = 50001;
    protected long maxRecursiveUrlsPerSitemapIndex = 50000L * 50000;

    protected int maxSitemapFetchTime = 180;
    protected int maxSitemapProcessingTime;
    protected int maxUrlLength = 512;

    protected boolean checkRobotsTxt = true;
    protected int maxFailuresPerHost = 5;

    private ProtocolFactory protocolFactory;
    private SiteMapParser sitemapParser;
    private ExecutorService executorService;
    private Map<String,Integer> failuresPerHost = new HashMap<>();
    
    public void configure(JobConf job) {
      super.configure(job);

      protocolFactory = new ProtocolFactory(job);

      // SiteMapParser to allow "cross submits" from different prefixes
      // (up to last slash), cf. http://www.sitemaps.org/protocol.html#location
      // strict = true : do not allow
      // TODO: need to pass a set of cross-submit allowed hosts
      boolean strict = jobConf.getBoolean("db.injector.sitemap.strict", false);
      sitemapParser = new SiteMapParser(strict, true);

      maxRecursiveSitemaps = jobConf.getInt("db.injector.sitemap.index_max_size", 50001);
      maxRecursiveUrlsPerSitemapIndex = jobConf.getLong(SITEMAP_MAX_URLS, 50000L * 50000);

      checkRobotsTxt = jobConf.getBoolean("db.injector.sitemap.checkrobotstxt", true);

      // make sure a sitemap is entirely, even recursively processed within 80%
      // of the task timeout, do not start processing a subsitemap if fetch
      // and parsing time may hit the task timeout
      int taskTimeout = jobConf.getInt("mapreduce.task.timeout", 900000) / 1000;
      maxSitemapProcessingTime = taskTimeout - (2 * maxSitemapFetchTime);
      if ((taskTimeout * .8) < maxSitemapProcessingTime) {
        maxSitemapProcessingTime = (int) (taskTimeout * .8);
      }
      maxFailuresPerHost = jobConf.getInt("db.injector.sitemap.max.fetch.failures.per.host", 5);

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

      float customScore = 0.0f;
      long maxUrls = maxRecursiveUrlsPerSitemapIndex;
      Metadata customMetadata = new Metadata();
      if (url.contains("\t") || url.contains(" ")){
        String[] splits;
        splits = url.split("[\t ]");
        if (splits.length == 0) {
          LOG.warn("Empty line (white space only): <{}>", url);
          return;
        }
        url = splits[0];
        for (String split : splits) {
          int indexEquals = split.indexOf("=");
          if (indexEquals==-1)
            continue;
          String metaname = split.substring(0, indexEquals);
          String metavalue = split.substring(indexEquals+1);
          if (metaname.equals(nutchScoreMDName)
              || metaname.equals(ccScoreMDName)) {
            try {
              customScore = Float.parseFloat(metavalue);
            } catch (NumberFormatException nfe) {
              LOG.error("Invalid custom score for sitemap seed {}: {} - {}",
                  url, metavalue, nfe.getMessage());
            }
          } else if (metaname.equals(SITEMAP_MAX_URLS)) {
            try {
              maxUrls = Long.parseLong(metavalue);
              LOG.info("Setting max. number of URLs per sitemap for {} = {}",
                  url, maxUrls);
            } catch (NumberFormatException nfe) {
              LOG.error("Invalid URL limit for sitemap seed {}: {} - {}",
                  url, metavalue, nfe.getMessage());
            }
          } else {
            customMetadata.add(metaname,metavalue);
          }
        }
      }
      // distribute site score to outlinks
      // TODO: should be by real number of outlinks not the maximum allowed
      customScore /= maxUrls;

      long startTime = System.currentTimeMillis();

      Content content = getContent(url, reporter);
      if (content == null) {
        return;
      }

      try {
        parseProcessSitemap(content, url, output, reporter, startTime, maxUrls,
            customScore);
      } catch (Exception e) {
        LOG.warn("Failed to process sitemap {}: {}", url,
            StringUtils.stringifyException(e));
      }
    }

    class FetchSitemapCallable implements Callable<ProtocolOutput> {
      private Protocol protocol;
      private String url;
      private Reporter reporter;

      public FetchSitemapCallable(Protocol protocol, String url, Reporter reporter) {
        this.protocol = protocol;
        this.url = url;
        this.reporter = reporter;
      }

      @Override
      public ProtocolOutput call() throws Exception {
        Text turl = new Text(url);
        if (checkRobotsTxt) {
          BaseRobotRules rules = protocol.getRobotRules(turl, null, null);
          if (!rules.isAllowed(url)) {
            LOG.info("Fetch of sitemap forbidden by robots.txt: {}", url);
            reporter
              .getCounter("SitemapInjector", "failed to fetch sitemap content, robots.txt disallow")
              .increment(1);
            return null;
          }
        }
        return protocol.getProtocolOutput(turl, new CrawlDatum());
      }
    }

    class ParseSitemapCallable implements Callable<AbstractSiteMap> {
      private Content content;
      private String url;
      private AbstractSiteMap sitemap;

      public ParseSitemapCallable(Content content, Object urlOrSitemap) {
        this.content = content;
        if (urlOrSitemap instanceof String)
          this.url = (String) urlOrSitemap;
        else if (urlOrSitemap instanceof AbstractSiteMap)
          this.sitemap = (AbstractSiteMap) urlOrSitemap;
        else
          throw new IllegalArgumentException(
              "URL (String) or sitemap (AbstractSiteMap) required as argument");
      }

      @Override
      public AbstractSiteMap call() throws Exception {
        if (sitemap != null) {
          return sitemapParser.parseSiteMap(content.getContentType(),
              content.getContent(), sitemap);
        } else {
          return sitemapParser.parseSiteMap(content.getContentType(),
              content.getContent(), new URL(url));
        }
      }
    }

    class ScoredSitemap implements Comparable<ScoredSitemap> {
      double score;
      AbstractSiteMap sitemap;
      public ScoredSitemap(double score, AbstractSiteMap sitemap) {
        this.score = score;
        this.sitemap = sitemap;
      }
      @Override
      public int compareTo(ScoredSitemap other) {
        return Double.compare(other.score, this.score);
      }
    }

    private void incrementFailuresPerHost(String hostName) {
      int failures = 1;
      if (failuresPerHost.containsKey(hostName)) {
        failures += failuresPerHost.get(hostName);
      }
      failuresPerHost.put(hostName, failures);
    }

    private Content getContent(String url, Reporter reporter) {
      String hostName;
      try {
        hostName = new URL(url).getHost();
      } catch (MalformedURLException e1) {
        return null;
      }
      if (failuresPerHost.containsKey(hostName)
          && failuresPerHost.get(hostName) > maxFailuresPerHost) {
        LOG.info("Skipped, too many failures per host: {}", url);
        reporter
          .getCounter("SitemapInjector", "skipped, too many failures per host")
          .increment(1);
        return null;
      }
      Protocol protocol = null;
      try {
        protocol = protocolFactory.getProtocol(url);
      } catch (ProtocolNotFound e) {
        LOG.error("protocol not found " + url);
        reporter
          .getCounter("SitemapInjector", "failed to fetch sitemap content, protocol not found")
          .increment(1);
        return null;
      }

      LOG.info("fetching sitemap " + url);
      FetchSitemapCallable fetch = new FetchSitemapCallable(protocol, url, reporter);
      Future<ProtocolOutput> task = executorService.submit(fetch);
      ProtocolOutput protocolOutput = null;
      try {
        protocolOutput = task.get(maxSitemapFetchTime, TimeUnit.SECONDS);
      } catch (Exception e) {
        if (e instanceof TimeoutException) {
          LOG.error("fetch of sitemap {} timed out", url);
          reporter.getCounter("SitemapInjector",
              "failed to fetch sitemap content, timeout").increment(1);
        } else {
          LOG.error("fetch of sitemap {} failed with: {}", url,
              StringUtils.stringifyException(e));
          reporter.getCounter("SitemapInjector",
              "failed to fetch sitemap content, exception").increment(1);
        }
        task.cancel(true);
        incrementFailuresPerHost(hostName);
        return null;
      } finally {
        fetch = null;
      }

      if (protocolOutput == null) {
        return null;
      }
      if (ProtocolStatus.STATUS_SUCCESS != protocolOutput.getStatus()) {
        // TODO: follow redirects
        LOG.error("fetch of sitemap {} failed with status code {}", url,
            protocolOutput.getStatus().getCode());
        reporter
          .getCounter("SitemapInjector", "failed to fetch sitemap content, HTTP status != 200")
          .increment(1);
        incrementFailuresPerHost(hostName);
        return null;
      }
      Content content = protocolOutput.getContent();
      if (content == null) {
        LOG.error("No content for {}, status: {}", url,
            protocolOutput.getStatus().getMessage());
        reporter
          .getCounter("SitemapInjector", "failed to fetch sitemap content, empty content")
          .increment(1);
        incrementFailuresPerHost(hostName);
        return null;
      }
      return content;
    }

    private AbstractSiteMap parseSitemap(Content content, Object urlOrSitemap)
        throws Exception {
      ParseSitemapCallable parse = new ParseSitemapCallable(content,
          urlOrSitemap);
      Future<AbstractSiteMap> task = executorService.submit(parse);
      AbstractSiteMap sitemap = null;
      try {
        // not a recursive task, should be fast
        sitemap = task.get(maxSitemapProcessingTime/20, TimeUnit.SECONDS);
      } finally {
        parse = null;
      }
      return sitemap;
    }

    /**
     * Within limited time: parse and process a sitemap (recursively, in case of
     * a sitemap index) and inject URLs
     */
    private void parseProcessSitemap(Content content, String url,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter,
        final long startTime, final long maxUrls, final float customScore) {

      AbstractSiteMap sitemap = null;
      try {
        sitemap = parseSitemap(content, url);
      } catch (Exception e) {
        reporter.getCounter("SitemapInjector", "sitemaps failed to parse").increment(1);
        LOG.warn("failed to parse sitemap {}: {}", url,
            StringUtils.stringifyException(e));
        return;
      }
      LOG.info("parsed sitemap {} ({})", url, sitemap.getType());

      AtomicLong injectedURLs = new AtomicLong(0);
      try {
        processSitemap(sitemap, output, reporter, injectedURLs, null,
            startTime, maxUrls, customScore);
      } catch (IOException e) {
        LOG.warn("failed to process sitemap {}: {}", url,
            StringUtils.stringifyException(e));
      }
      LOG.info("Injected total {} URLs for {}", injectedURLs, url);

    }

    /**
     * parse a sitemap (recursively, in case of a sitemap index),
     * and inject all contained URLs
     */
    public void processSitemap(AbstractSiteMap sitemap,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter,
        AtomicLong totalUrls, Set<String> processedSitemaps,
        final long startTime, final long maxUrls, final float customScore)
        throws IOException {

      if (sitemap.isIndex()) {
        SiteMapIndex sitemapIndex = (SiteMapIndex) sitemap;
        if (processedSitemaps == null) {
          processedSitemaps = new HashSet<String>();
          processedSitemaps.add(sitemap.getUrl().toString());
        }

        // choose subsitemaps randomly with a preference for elements in front
        PriorityQueue<ScoredSitemap> sitemaps = new PriorityQueue<>();
        int subSitemaps = 0;
        for (AbstractSiteMap s : sitemapIndex.getSitemaps()) {
          subSitemaps++;
          double score = (1.0/subSitemaps) + Math.random();
          sitemaps.add(new ScoredSitemap(score, s));
        }

        int failedSubSitemaps = 0;
        while (sitemaps.size() > 0) {

          long elapsed = (System.currentTimeMillis()-startTime)/1000;
          if (elapsed > maxSitemapProcessingTime) {
            LOG.warn(
                "Max. processing time reached, skipped remaining sitemaps of sitemap index {}",
                sitemap.getUrl());
            reporter.getCounter("SitemapInjector",
                "sitemap index: time limit reached").increment(1);
            return;
          }
          if ((totalUrls.get() == 0)
              && (elapsed > (maxSitemapProcessingTime / 2))) {
            LOG.warn(
                "Half of processing time elapsed and no URLs injected, skipped remaining sitemaps of sitemap index {}",
                sitemap.getUrl());
            reporter
                .getCounter("SitemapInjector",
                    "sitemap index: no URLs after 50% of time limit")
                .increment(1);
            return;
          }
          if (failedSubSitemaps > (maxRecursiveSitemaps / 2)) {
            // do not spend too much time to fetch broken subsitemaps
            LOG.warn(
                "Too many failures, skipped remaining sitemaps of sitemap index {}",
                sitemap.getUrl());
            reporter
                .getCounter("SitemapInjector",
                    "sitemap index: too many failures")
                .increment(1);
            return;
          }

          AbstractSiteMap nextSitemap = sitemaps.poll().sitemap;
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
          if (totalUrls.longValue() >= maxUrls) {
            LOG.warn(
                "URL limit reached, skipped remaining sitemaps of sitemap index {}",
                sitemap.getUrl());
            reporter.getCounter("SitemapInjector",
                "sitemap index: URL limit reached").increment(1);
            return;
          }

          processedSitemaps.add(url);

          Content content = getContent(url, reporter);
          if (content == null) {
            nextSitemap.setProcessed(true);
            reporter.getCounter("SitemapInjector", "sitemaps failed to fetch")
                .increment(1);
            failedSubSitemaps++;
            continue;
          }

          try {
            AbstractSiteMap parsedSitemap = parseSitemap(content, nextSitemap);
            processSitemap(parsedSitemap, output, reporter, totalUrls,
                processedSitemaps, startTime, maxUrls, customScore);
          } catch (Exception e) {
            LOG.warn("failed to parse sitemap {}: {}", nextSitemap.getUrl(),
                StringUtils.stringifyException(e));
            reporter.getCounter("SitemapInjector", "sitemaps failed to parse")
                .increment(1);
            failedSubSitemaps++;
          }
          nextSitemap.setProcessed(true);
        }

      } else {
        reporter.getCounter("SitemapInjector", "sitemaps processed").increment(1);
        injectURLs((SiteMap) sitemap, output, reporter, totalUrls, maxUrls, customScore);
        if (totalUrls.longValue() >= maxUrls) {
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
        AtomicLong totalUrls, long maxUrls, float customScore)
        throws IOException {

      Collection<SiteMapURL> sitemapURLs = sitemap.getSiteMapUrls();
      if (sitemapURLs.size() == 0) {
        LOG.info("No URLs in sitemap {}", sitemap.getUrl());
        reporter.getCounter("SitemapInjector", "empty sitemap").increment(1);
        return;
      }
      LOG.info("Found {} URLs in {}", sitemapURLs.size(), sitemap.getUrl());

      // random selection of URLs in case the sitemap contains more than accepted
      // TODO:
      //  - for sitemap index: should be done over multiple sub-sitemaps
      //  - need to consider that URLs may be filtered away
      //  => use "reservoir sampling" (https://en.wikipedia.org/wiki/Reservoir_sampling)
      Random random = null;
      float randomSelect = 0.0f;
      if (sitemapURLs.size() > (maxUrls - totalUrls.get())) {
        randomSelect = (maxUrls - totalUrls.get())
            / (.95f * sitemapURLs.size());
        if (randomSelect < 1.0f) {
          random = new Random();
        }
      }

      for (SiteMapURL siteMapURL : sitemapURLs) {

        if (totalUrls.longValue() >= maxUrls) {
          reporter.getCounter("SitemapInjector", "sitemap URL limit reached").increment(1);
          return;
        }

        if (random != null) {
          if (randomSelect > random.nextFloat()) {
            reporter.getCounter("SitemapInjector", "random skip").increment(1);
            continue;
          }
        }

        totalUrls.incrementAndGet();

        // TODO: score and fetch interval should be transparently overridable
        float sitemapScore = (float) siteMapURL.getPriority();
        sitemapScore *= customScore;
        int sitemapInterval = getChangeFrequencySeconds(siteMapURL.getChangeFrequency());
        long lastModified = -1;
        if (siteMapURL.getLastModified() != null) {
          lastModified = siteMapURL.getLastModified().getTime();
        }

        String url = siteMapURL.getUrl().toString();
        if (url.length() > maxUrlLength) {
          LOG.warn(
              "Skipping overlong URL: {} ... (truncated, length = {} characters)",
              url.substring(0, maxUrlLength), url.length());
          continue;
        }
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
              sitemapInterval, sitemapScore);
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
    sortJob.setMapRunnerClass(MultithreadedMapRunner.class);

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
