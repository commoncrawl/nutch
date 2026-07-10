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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A more compact, read-only alternative to {@link SortedHostProtocolTable}: the
 * host names are melted into a single byte array (the <em>blob</em>) instead of
 * kept as individual {@code String} objects, and lookups are an unsigned binary
 * search over that blob.
 *
 * <p>
 * Three flat arrays hold the exact-host rules:
 * <ul>
 * <li>{@code hostBlob} &mdash; all host names, each with its bytes reversed,
 * concatenated without separators and sorted in unsigned byte order;</li>
 * <li>{@code hostOffsets} &mdash; {@code N+1} offsets; reversed host {@code i}
 * occupies {@code hostBlob[hostOffsets[i] .. hostOffsets[i+1])};</li>
 * <li>{@code hostProtocol} &mdash; per host, an unsigned-byte index into
 * {@code protocolDict}.</li>
 * </ul>
 * Because the host names no longer exist as {@code String} objects, this drops
 * both the {@code String} headers and their backing-array headers that
 * {@link SortedHostProtocolTable} still pays &mdash; roughly halving the
 * footprint again (on a ~6.75M-rule set: ~447 MiB down to ~150 MiB).
 *
 * <p>
 * Hosts are stored <em>reversed</em> so shared TLD/domain suffixes cluster
 * together, leaving room for a later front-coding upgrade without changing the
 * lookup contract; for plain exact matching the reversal is behavior-neutral.
 * Both the build-time sort and {@link #search(byte[])} order hosts with the
 * JDK's <em>unsigned</em> lexicographic comparison, so the array the search
 * walks is sorted in exactly the order the search assumes &mdash; the one
 * correctness invariant that keeps non-ASCII (e.g. punycode) hosts findable.
 */
final class SortedBinaryHostProtocolTable {

  // reversed host names concatenated, sorted in unsigned byte order
  private final byte[] hostBlob;
  // N+1 offsets; reversed host i occupies hostBlob[hostOffsets[i] .. [i+1])
  private final int[] hostOffsets;
  // N entries; unsigned-byte index into protocolDict (up to 255 protocols)
  private final byte[] hostProtocol;
  // distinct protocol strings, e.g. ["http", "https"]
  private final String[] protocolDict;

  // A parsed exact-host rule held only during the build: the reversed host
  // bytes and the protocol-dictionary index. Sorted by reversed host bytes to
  // become the blob.
  private static final class Entry {
    final byte[] reversedHost;
    final int protocolIndex;

    Entry(byte[] reversedHost, int protocolIndex) {
      this.reversedHost = reversedHost;
      this.protocolIndex = protocolIndex;
    }
  }

  /**
   * Build the table from a map of {@code host -> protocol}. Duplicate hosts are
   * already collapsed by the map (last-wins). Protocols are interned into a
   * small dictionary and referenced per host by a one-byte index.
   *
   * @param rules
   *          host to protocol
   */
  SortedBinaryHostProtocolTable(Map<String, String> rules) {
    // intern protocols to dictionary indices
    Map<String, Integer> protocolIndex = new HashMap<>();
    List<String> protocolList = new ArrayList<>();

    int n = rules.size();
    Entry[] entries = new Entry[n];
    int k = 0;
    for (Map.Entry<String, String> rule : rules.entrySet()) {
      String protocol = rule.getValue();
      Integer protoIdx = protocolIndex.get(protocol);
      if (protoIdx == null) {
        protoIdx = protocolList.size();
        protocolIndex.put(protocol, protoIdx);
        protocolList.add(protocol);
      }
      byte[] reversed = reverse(
          rule.getKey().getBytes(StandardCharsets.UTF_8));
      entries[k++] = new Entry(reversed, protoIdx);
    }

    // The dictionary index is stored as an unsigned byte; guard the (absurd for
    // this domain) case of more than 255 distinct protocols rather than
    // silently truncating it.
    if (protocolList.size() > 255) {
      throw new IllegalArgumentException(
          "Too many distinct protocols (" + protocolList.size()
              + "); at most 255 are supported");
    }
    this.protocolDict = protocolList.toArray(new String[0]);

    // sort by reversed host bytes, unsigned -- the SAME order search() uses
    Arrays.sort(entries,
        (a, b) -> Arrays.compareUnsigned(a.reversedHost, b.reversedHost));

    long total = 0;
    for (Entry e : entries) {
      total += e.reversedHost.length;
    }
    if (total > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Exact-host rules exceed the maximum blob size of 2 GiB");
    }

    this.hostBlob = new byte[(int) total];
    this.hostOffsets = new int[n + 1];
    this.hostProtocol = new byte[n];
    int pos = 0;
    for (int i = 0; i < n; i++) {
      Entry e = entries[i];
      hostOffsets[i] = pos;
      System.arraycopy(e.reversedHost, 0, hostBlob, pos, e.reversedHost.length);
      pos += e.reversedHost.length;
      hostProtocol[i] = (byte) e.protocolIndex;
    }
    hostOffsets[n] = pos;
  }

  /**
   * @return the protocol configured for {@code host}, or {@code null} if there
   *         is no exact rule for it.
   */
  String get(String host) {
    int idx = search(reverse(host.getBytes(StandardCharsets.UTF_8)));
    // negative == not found; a miss must return null so the caller leaves the
    // URL unchanged, never index the arrays with it
    return idx >= 0 ? protocolDict[hostProtocol[idx] & 0xFF] : null;
  }

  /** Number of exact-host rules held (handy for tests/sanity checks). */
  int size() {
    return hostProtocol.length;
  }

  /** Reverse the bytes of {@code b} in place and return it. */
  private static byte[] reverse(byte[] b) {
    for (int i = 0, j = b.length - 1; i < j; i++, j--) {
      byte t = b[i];
      b[i] = b[j];
      b[j] = t;
    }
    return b;
  }

  /**
   * Unsigned binary search for the reversed query bytes {@code q} over the
   * sorted host blob. Returns the host index, or -1 if absent.
   */
  private int search(byte[] q) {
    int lo = 0;
    int hi = hostProtocol.length - 1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      int cmp = Arrays.compareUnsigned(hostBlob, hostOffsets[mid],
          hostOffsets[mid + 1], q, 0, q.length);
      if (cmp < 0) {
        lo = mid + 1;
      } else if (cmp > 0) {
        hi = mid - 1;
      } else {
        return mid;
      }
    }
    return -1;
  }
}
