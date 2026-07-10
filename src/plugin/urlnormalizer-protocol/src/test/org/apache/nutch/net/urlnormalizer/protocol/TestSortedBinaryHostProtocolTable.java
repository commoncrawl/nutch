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

class TestSortedBinaryHostProtocolTable {

  private static Map<String, String> rules(String... hostProto) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i < hostProto.length; i += 2) {
      m.put(hostProto[i], hostProto[i + 1]);
    }
    return m;
  }

  /**
   * A host that has a rule must return its OWN protocol -- the basic contract
   * the normalizer relies on to rewrite the scheme.
   */
  @Test
  void exactHitReturnsThatHostsProtocol() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("example.com", "https", "example.org", "http"));

    assertEquals("https", t.get("example.com"));
    assertEquals("http", t.get("example.org"));
  }

  /**
   * A host with no rule must return null (not crash). The search returns -1 on
   * a miss, and most hostnames on a real crawl have no rule, so this is the
   * common path -- it must leave the URL unchanged.
   */
  @Test
  void missReturnsNullNotCrash() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("example.com", "https"));

    assertNull(t.get("absent.example"));
  }

  /**
   * A host that sorts strictly BETWEEN two rules but is itself absent must
   * miss; catches an off-by-one where a near-neighbour is wrongly returned.
   */
  @Test
  void missBetweenTwoRulesReturnsNull() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("a.example", "https", "c.example", "https"));

    assertNull(t.get("b.example"));
  }

  /**
   * The protocol must follow its host through the internal sort. Insertion
   * order here is the reverse of sorted order and the two hosts have different
   * protocols, so a build that lost the host/protocol pairing would fail.
   */
  @Test
  void protocolStaysAlignedToHostAfterSorting() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("zzz.example", "http", "aaa.example", "https"));

    assertEquals("https", t.get("aaa.example"));
    assertEquals("http", t.get("zzz.example"));
  }

  /**
   * The caller must not pre-sort: an arbitrarily-ordered HashMap must produce a
   * correctly searchable table (the table sorts itself at build time).
   */
  @Test
  void unsortedInputMapIsHandled() {
    Map<String, String> m = new HashMap<>();
    m.put("m.example", "https");
    m.put("a.example", "http");
    m.put("z.example", "https");
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(m);

    assertEquals("http", t.get("a.example"));
    assertEquals("https", t.get("m.example"));
    assertEquals("https", t.get("z.example"));
    assertNull(t.get("n.example"));
  }

  /**
   * An empty rule set must build and answer every lookup with null, never
   * blowing up on the empty arrays (search over a zero-length blob).
   */
  @Test
  void emptyTableMissesEverything() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        new HashMap<String, String>());

    assertEquals(0, t.size());
    assertNull(t.get("anything.example"));
  }

  /**
   * THE key test for the blob: a non-ASCII (raw UTF-8) host. The "\u00e9" is bytes
   * 0xC3 0xA9 -- negative as signed bytes -- so if the sort or the search used
   * signed comparison instead of {@code Arrays.compareUnsigned}, the host would
   * sort out of order and the binary search would silently miss it. The host
   * must still be found; ASCII neighbours pin the ordering on both sides. (The
   * "\u00e9" is written as a unicode escape so the test does not depend on the
   * source file encoding.)
   */
  @Test
  void nonAsciiHostRoundTrips() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("aaa.example", "https", "caf\u00e9.example", "https",
            "zzz.example", "https"));

    assertEquals("https", t.get("caf\u00e9.example"));
    assertEquals("https", t.get("aaa.example"));
    assertEquals("https", t.get("zzz.example"));
    assertNull(t.get("bbb.example"));
  }

  /**
   * More than two distinct protocols exercises the protocol dictionary index
   * (a single stolen bit would not suffice; the byte index must round-trip).
   */
  @Test
  void moreThanTwoProtocols() {
    SortedBinaryHostProtocolTable t = new SortedBinaryHostProtocolTable(
        rules("a.example", "http", "b.example", "https", "c.example", "ftp"));

    assertEquals("http", t.get("a.example"));
    assertEquals("https", t.get("b.example"));
    assertEquals("ftp", t.get("c.example"));
  }
}
