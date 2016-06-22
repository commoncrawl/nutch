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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FetchSchedule which allows to reset the fetch interval to the default value
 */
public class CCFetchSchedule extends DefaultFetchSchedule {

  private final static Logger LOG = LoggerFactory
      .getLogger(CCFetchSchedule.class);

  private static final String RESET_FETCH_INTERVAL = "db.fetch.interval.reset.default";

  private boolean resetFetchInterval;

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null)
      return;
    resetFetchInterval = conf.getBoolean(RESET_FETCH_INTERVAL, false);
    LOG.info("{} = {}", RESET_FETCH_INTERVAL, resetFetchInterval);
  }

  @Override
  public CrawlDatum setPageGoneSchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    if (resetFetchInterval && datum.getFetchInterval() != defaultInterval) {
      datum.setFetchInterval(defaultInterval);
    }
    return super.setPageGoneSchedule(url, datum, prevFetchTime,
        prevModifiedTime, fetchTime);
  }

  @Override
  public CrawlDatum setPageRetrySchedule(Text url, CrawlDatum datum,
      long prevFetchTime, long prevModifiedTime, long fetchTime) {
    if (resetFetchInterval && datum.getFetchInterval() != defaultInterval) {
      datum.setFetchInterval(defaultInterval);
    }
    return super.setPageRetrySchedule(url, datum, prevFetchTime,
        prevModifiedTime, fetchTime);
  }

  @Override
  public CrawlDatum setFetchSchedule(Text url, CrawlDatum datum,
          long prevFetchTime, long prevModifiedTime,
          long fetchTime, long modifiedTime, int state) {
    if (resetFetchInterval && datum.getFetchInterval() != defaultInterval) {
      datum.setFetchInterval(defaultInterval);
    }
    return super.setFetchSchedule(url, datum, prevFetchTime, prevModifiedTime,
        fetchTime, modifiedTime, state);
  }
}
