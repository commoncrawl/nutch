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
package org.apache.nutch.net.urlnormalizer.protocol;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 * A compact, read-only replacement for the exact-host {@code HashMap}: it holds
 * the host names in one sorted {@code String[]} and their protocols in a
 * parallel {@code String[]}, and answers lookups with binary search.
 *
 * <p>
 * Built once from the complete rule set (no incremental {@code put}), which is
 * exactly why it can stay sorted and packed. Drops the per-entry
 * {@code HashMap$Node} and bucket slack, so ~700 MB of exact-host rules become
 * ~430 MB at identical behavior.
 */
final class SortedHostProtocolTable {

  // host names in ascending natural (String.compareTo) order
  private final String[] hosts;
  // protocols[i] is the protocol for hosts[i] (interned upstream, so equal
  // protocol strings are shared references and cost almost nothing)
  private final String[] protocols;

  /**
   * Build the table from a map of {@code host -> protocol}. The two parallel
   * arrays must come out sorted by host and aligned (protocols[i] belongs to
   * hosts[i]).
   *
   * @param rules
   *          host to protocol; values should already be interned by the caller
   */
  SortedHostProtocolTable(Map<String, String> rules) {
    Map<String, String> sorted = rules;
    if (!(rules instanceof TreeMap )){
      sorted =  new TreeMap<>(rules);
    }

    int n = sorted.size();
    this.hosts = new String[n];
    this.protocols = new String[n];
    int i = 0;
    for (Map.Entry<String, String> rule : sorted.entrySet()) {
      hosts[i] = rule.getKey();
      protocols[i] = rule.getValue();
      i++;
    }
  }

  /**
   * @return the protocol configured for {@code host}, or {@code null} if there
   *         is no exact rule for it.
   */
  String get(String host) {
      int idx = Arrays.binarySearch(hosts, host);
    // negative == not found (binarySearch returns -(insertionPoint)-1); a miss
    // must return null so the caller leaves the URL alone -- never index with it
      return idx >= 0 ? protocols[idx] : null;
  }

  /** Number of exact-host rules held (handy for tests/sanity checks). */
  int size() {
    return hosts.length;
  }
}
