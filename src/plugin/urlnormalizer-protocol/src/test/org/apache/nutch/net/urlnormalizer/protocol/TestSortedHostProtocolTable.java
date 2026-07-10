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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestSortedHostProtocolTable {

  private static Map<String, String> rules(String... hostProto) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < hostProto.length; i += 2) {
      m.put(hostProto[i], hostProto[i + 1]);
    }
    return m;
  }

  /**
   * A host that has a rule must return its OWN protocol. This is the basic
   * contract the normalizer relies on to rewrite the scheme.
   */
  @Test
  void exactHitReturnsThatHostsProtocol() {
    SortedHostProtocolTable t = new SortedHostProtocolTable(
        rules("example.com", "https", "example.org", "http"));

    assertEquals("https", t.get("example.com"));
    assertEquals("http", t.get("example.org"));
  }

  /**
   * A host with no rule must return null (not crash). This is the important
   * one: binarySearch returns a NEGATIVE index for a miss, and most hostnames
   * on a real crawl have no rule -- if a miss indexed the array it would throw
   * on the common path. Null lets the normalizer leave the URL unchanged.
   */
  @Test
  void missReturnsNullNotCrash() {
    SortedHostProtocolTable t = new SortedHostProtocolTable(
        rules("example.com", "https"));

    assertNull(t.get("absent.example"));
  }

  /**
   * A host that sorts strictly BETWEEN two rules but is itself absent must miss.
   * binarySearch's insertion point lands mid-array here, so this catches an
   * off-by-one where a near-neighbour is wrongly returned.
   */
  @Test
  void missBetweenTwoRulesReturnsNull() {
    SortedHostProtocolTable t = new SortedHostProtocolTable(
        rules("a.example", "https", "c.example", "https"));

    assertNull(t.get("b.example"));
  }

  /**
   * The protocol must follow its host THROUGH the sort. Insertion order here is
   * the reverse of sorted order and the two hosts have different protocols, so
   * an implementation that sorted the hosts but not the protocols (misaligned
   * arrays) would return the wrong protocol and fail this test.
   */
  @Test
  void protocolStaysAlignedToHostAfterSorting() {
    // inserted zzz first, aaa second; sorted order is aaa < zzz
    SortedHostProtocolTable t = new SortedHostProtocolTable(
        rules("zzz.example", "http", "aaa.example", "https"));

    assertEquals("https", t.get("aaa.example"));
    assertEquals("http", t.get("zzz.example"));
  }

  /**
   * The caller must not have to pre-sort: an arbitrarily-ordered HashMap must
   * produce a correctly searchable table (the constructor owns the sorting).
   */
  @Test
  void unsortedInputMapIsHandled() {
    Map<String, String> m = new HashMap<>();
    m.put("m.example", "https");
    m.put("a.example", "http");
    m.put("z.example", "https");
    SortedHostProtocolTable t = new SortedHostProtocolTable(m);

    assertEquals("http", t.get("a.example"));
    assertEquals("https", t.get("m.example"));
    assertEquals("https", t.get("z.example"));
    assertNull(t.get("n.example"));
  }

  /**
   * An empty rule set must build and answer every lookup with null, never
   * blowing up on the empty arrays.
   */
  @Test
  void emptyTableMissesEverything() {
    SortedHostProtocolTable t = new SortedHostProtocolTable(
        new HashMap<String, String>());

    assertEquals(0, t.size());
    assertNull(t.get("anything.example"));
  }
}
