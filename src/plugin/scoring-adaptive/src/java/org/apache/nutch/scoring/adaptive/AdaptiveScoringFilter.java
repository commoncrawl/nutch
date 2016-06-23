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

package org.apache.nutch.scoring.adaptive;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.scoring.AbstractScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * First step to an adaptive and dynamic scoring filter.
 */
public class AdaptiveScoringFilter extends AbstractScoringFilter {

  private final static Logger LOG = LoggerFactory
      .getLogger(AdaptiveScoringFilter.class);

  public static final String ADAPTIVE_FETCH_TIME_SORT_FACTOR = "db.score.adaptive.sort.by_status.file";

  public static final String ADAPTIVE_STATUS_SORT_FACTOR_FILE = "db.score.adaptive.factor.status.file";

  private Configuration conf;

  /**
   * Current time in milliseconds used to calculate time elapsed since a page
   * should have been (re)fetched while generating fetch lists. Can be
   * set/overwritten from {@link Generator} by option -adddays (internally set
   * via @{link Generator.GENERATOR_CUR_TIME}.
   */
  private long curTime;

  private float adaptiveFetchTimeSort;

  private Map<Byte, Float> statusSortMap = new TreeMap<Byte, Float>();

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
    curTime = conf.getLong(Generator.GENERATOR_CUR_TIME,
        System.currentTimeMillis());
    adaptiveFetchTimeSort = conf.getFloat(ADAPTIVE_FETCH_TIME_SORT_FACTOR, .05f);
    String adaptiveStatusSortFile = conf.get(ADAPTIVE_STATUS_SORT_FACTOR_FILE, "adaptive-scoring.txt");
    Reader adaptiveStatusSortReader = conf.getConfResourceAsReader(adaptiveStatusSortFile);
    try {
      readSortFile(adaptiveStatusSortReader);
    } catch (IOException e) {
      LOG.error("Failed to read adaptive scoring file {}: {}",
          adaptiveStatusSortFile, StringUtils.stringifyException(e));
    }
  }

  private void readSortFile (Reader sortFileReader) throws IOException {
    BufferedReader reader = new BufferedReader(sortFileReader);
    String line = null;
    String[] splits = null;
    while ((line = reader.readLine()) != null) {
      if (line.matches("^\\s*$") || line.startsWith("#"))
        continue; // skip empty lines and comments
      splits = line.split("\t");
      if (splits.length < 2) {
        LOG.warn("Invalid line (expected format <status> \t <sortval>): {}", line);
        continue;
      }
      float value;
      try {
        value = Float.parseFloat(splits[1]);
      } catch (NumberFormatException e) {
        LOG.warn("Invalid sort value `{}' in line: {}", splits[1], line);
        continue;
      }
      byte status = -1;
      for (Entry<Byte, String> entry : CrawlDatum.statNames.entrySet()) {
        if (entry.getValue().equals(splits[0])) {
          status = entry.getKey();
          statusSortMap.put(status, value);
          break;
        }
      }
      if (status == -1) {
        LOG.warn("Invalid status `{}' in line: {}", splits[0], line);
      }
    }

  }

  /**
   * Use {@link CrawlDatum#getScore()} but be adaptive to page status and
   * fetch time.
   */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    initSort *= datum.getScore();
    long fetchTime = datum.getFetchTime();
    if (adaptiveFetchTimeSort > 0.0f) {
      //long daysSinceScheduledFetch = (curTime - fetchTime) / 86400000;
      //long log2days = 63 - Long.numberOfLeadingZeros(1+daysSinceScheduledFetch);
      double daysSinceScheduledFetch = (curTime - fetchTime) / 86400000.0d;
      double log2days = Math.log1p(daysSinceScheduledFetch)/Math.log(2);
      float fetchTimeSort = (float) (adaptiveFetchTimeSort * log2days);
      initSort += fetchTimeSort;
    }
    byte status = datum.getStatus();
    if (statusSortMap.containsKey(status)) {
      initSort += statusSortMap.get(status);
    }
    return initSort;
  }

}
