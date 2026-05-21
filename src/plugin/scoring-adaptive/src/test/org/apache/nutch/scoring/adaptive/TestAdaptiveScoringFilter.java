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

import static org.junit.jupiter.api.Assertions.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.jupiter.api.Test;

public class TestAdaptiveScoringFilter {

  public ScoringFilter getFilter(float nonCanonicalPenalty) {
    Configuration conf = NutchConfiguration.create();
    conf.setFloat(AdaptiveScoringFilter.ADAPTIVE_NON_CANONICAL_PENALTY, nonCanonicalPenalty);

    ScoringFilter filter = new AdaptiveScoringFilter();
    filter.setConf(conf);
    return filter;
  }

  @Test
  public void testPenalizeNonCanonical() throws ScoringFilterException {
    String canonicalUrl = "https://example.org/";
    String nonCanonicalUrl = "https://www.example.org/";
    float score = .5f;
    float initSort = 1.0f;
    Text url1 = new Text(canonicalUrl);
    Text url2 = new Text(nonCanonicalUrl);
    CrawlDatum datum = new CrawlDatum();
    datum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
    datum.setScore(.5f);
    datum.getMetaData().put(Nutch.CANONICAL_LINK_KEY, url1);

    // test with zero penalty configured
    ScoringFilter filter = getFilter(.0f);
    assertEquals(score, filter.generatorSortValue(url1, datum, initSort),
        "With zero penalty, generator sort value should be equal to score");
    assertEquals(score, filter.generatorSortValue(url2, datum, initSort),
        "With zero penalty, generator sort value should be equal to score");

    // using a penalty, the canonical page should get a higher value
    float penalty = .07f;
    filter = getFilter(penalty);
    float valCanonical = filter.generatorSortValue(url1, datum, initSort);
    float valNonCanonical = filter.generatorSortValue(url2, datum, initSort);
    assertEquals(score, valCanonical,
        "For canonical pages, generator sort value should be equal to score");
    assertNotEquals(score, valNonCanonical,
        "For non-canonical pages, generator sort value should *not* be equal to score");
    assertTrue(score > valNonCanonical,
        "For non-canonical pages, generator sort value should be lower than score");
    assertEquals((score - penalty), valNonCanonical);

    // test with empty canonical link
    datum.getMetaData().put(Nutch.CANONICAL_LINK_KEY, NullWritable.get());
    assertEquals(score, filter.generatorSortValue(url1, datum, initSort),
        "Without canonical link, generator sort value should be equal to score");
    datum.getMetaData().remove(Nutch.CANONICAL_LINK_KEY);
    assertEquals(score, filter.generatorSortValue(url1, datum, initSort),
        "Without canonical link, generator sort value should be equal to score");
  }

}
