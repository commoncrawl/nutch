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

package org.apache.nutch.net.urlnormalizer.fast;

import java.net.URL;
import java.net.MalformedURLException;
import java.nio.CharBuffer;

// Slf4j Logging imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Nutch imports
import org.apache.nutch.net.URLNormalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.oro.text.regex.*;

/**
 * Converts URLs to a normal form .
 */
public class FastURLNormalizer extends Configured implements URLNormalizer {
  public static final Logger LOG = LoggerFactory.getLogger(FastURLNormalizer.class);

  static char slash[] = {'/'};
  static char dotSlash[] = {'.', '/'};
  static char dotDotSlash[] = {'.', '.', '/'};
  static char slashDotSlash[] = {'/', '.', '/'};
  static char slashSlash[] = {'/', '/'};
  static char slashDotDotSlash[] = {'/', '.', '.', '/'};

  private Configuration conf;

  public FastURLNormalizer() {}

  public String normalize(String urlString, String scope)
      throws MalformedURLException {
    if ("".equals(urlString))                     // permit empty
      return urlString;

    urlString = urlString.trim();                 // remove extra spaces

    URL url = new URL(urlString);

    String protocol = url.getProtocol();
    String host = url.getHost();
    int port = url.getPort();
    String file = url.getFile();

    boolean changed = false;

    if (!urlString.startsWith(protocol))        // protocol was lowercased
      changed = true;

    if ("http".equals(protocol) || "https".equals(protocol) || "ftp".equals(protocol)) {

      if (host != null) {
        String newHost = host.toLowerCase();    // lowercase host
        if (!host.equals(newHost)) {
          host = newHost;
          changed = true;
        }
      }

      if (port == url.getDefaultPort()) {       // uses default port
        port = -1;                              // so don't specify it
        changed = true;
      }

      if (file == null || "".equals(file)) {    // add a slash
        file = "/";
        changed = true;
      }

      if (url.getRef() != null) {                 // remove the ref
        changed = true;
      }

      // check for unnecessary use of "/../"
      String file2 = substituteUnnecessaryRelativePaths(file);

      if (!file.equals(file2)) {
        changed = true;
        file = file2;
      }

    }

    if (changed)
      urlString = new URL(protocol, host, port, file).toString();

    return urlString;
  }

  public String substituteUnnecessaryRelativePaths(String input) {
    CharSequence sequence = input;
    CharBuffer buffer = null;
    boolean modified = false;

    int indexOut = -1;
    int searchStart = 0;


    // remove all occurrences of '/./'
    while ((indexOut = indexOf(sequence, buffer, slashDotSlash, 0, slashDotSlash.length)) != -1) {
      if (!modified) {
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      searchStart = indexOut;
      // get sub sequence (advanced past pattern)
      buffer.position(indexOut + slashDotSlash.length);
      CharBuffer trailingSequence = buffer.slice();
      // and append it back into the source buffer ...
      buffer.position(indexOut);
      // append single slash replacement ...
      buffer.put('/');
      // and trailing string ...
      buffer.put(trailingSequence);
      // and reset limit ...
      buffer.limit(buffer.position());
      // and reset position to search start
      buffer.position(searchStart);
    }


    // now process occurrences of /../

    indexOut = -1;
    searchStart = 0;

    if (modified)
      buffer.position(0);

    while ((indexOut = indexOf(sequence, buffer, slashDotDotSlash, 0, slashDotDotSlash.length)) != -1) {
      if (!modified) {
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      // get sub sequence (advanced past pattern)
      buffer.position(indexOut + slashDotDotSlash.length);
      CharBuffer trailingSequence = buffer.slice();
      // now walk backwards to previous occurrence of /
      int previousPos = indexOut;
      while (--previousPos >= 0) {
        if (buffer.get(previousPos) == '/')
          break;
      }
      // now if we found it ...
      if (previousPos != -1) {
        searchStart = previousPos;
        // set position to new location ...
        buffer.position(previousPos + 1);
      } else {
        searchStart = indexOut;
        // otherwise set position to indexout (just replace pattern itself)...
        buffer.position(indexOut);
        // append single slash replacement ...
        buffer.put('/');
      }
      // and trailing string ...
      buffer.put(trailingSequence);
      // and reset limit ...
      buffer.limit(buffer.position());
      // and reset position to search start
      buffer.position(searchStart);
    }

    if (modified)
      buffer.position(0);
    // now remove leading all leading ./
    while ((indexOut = indexOf(sequence, buffer, dotSlash, 0, dotSlash.length)) == 0) {
      if (!modified) {
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      replaceCharsAt(buffer, indexOut, dotSlash.length, slash);
      buffer.position(0);
    }

    if (modified)
      buffer.position(0);
    // and leading dot dot slashes ../
    while ((indexOut = indexOf(sequence, buffer, dotDotSlash, 0, dotDotSlash.length)) == 0) {
      if (!modified) {
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      replaceCharsAt(buffer, indexOut, dotDotSlash.length, slash);
      buffer.position(0);
    }

    if (modified)
      buffer.position(0);

    // remove all occurrences of '//'
    while ((indexOut = indexOf(sequence, buffer, slashSlash, 0, slashSlash.length)) != -1) {
      if (!modified) {
        buffer = copyString(input);
        modified = true;
        sequence = buffer;
      }
      searchStart = indexOut;
      // get sub sequence (advanced past pattern)
      buffer.position(indexOut + slashSlash.length);
      CharBuffer trailingSequence = buffer.slice();
      // and append it back into the source buffer ...
      buffer.position(indexOut);
      // append single slash replacement ...
      buffer.put('/');
      // and trailing string ...
      buffer.put(trailingSequence);
      // and reset limit ...
      buffer.limit(buffer.position());
      // and reset position to search start
      buffer.position(searchStart);
    }


    if (modified) {
      //return modified content...
      // reset position ...
      buffer.position(0);
      return buffer.toString();
    } else {
      // return original string ...
      return input;
    }
  }

  static void replaceCharsAt(CharBuffer buffer, int start, int count, char[] newCharSequence) {
    if (count > newCharSequence.length) {
      buffer.position(start + count);
      CharBuffer trailingSequence = buffer.slice();
      // position past new char sequence ...
      buffer.position(start + newCharSequence.length);
      buffer.put(trailingSequence);
    }
    // reset to start position ...
    buffer.position(start);
    for (char c : newCharSequence) {
      buffer.put(c);
    }
    if (count > newCharSequence.length) {
      buffer.limit(buffer.limit() - (count - newCharSequence.length));
    } else {
      buffer.limit(buffer.limit() + (newCharSequence.length - count));
    }
  }

  /**
   * Code shared by String and StringBuffer to do searches. The
   * source is the character array being searched, and the target
   * is the string being searched for.
   *
   * @param source       the characters being searched.
   * @param buffer       ?
   * @param target       the characters being searched for.
   * @param targetOffset offset of the target string.
   * @param targetCount  count of the target string.
   */
  static int indexOf(CharSequence source, CharBuffer buffer, char[] target, int targetOffset, int targetCount) {
    if (targetCount == 0) {
      return -1;
    }

    int sourceOffset = 0;
    int sourceCount = source.length();

    char first = target[targetOffset];
    int max = sourceOffset + (sourceCount - targetCount);

    for (int i = sourceOffset; i <= max; i++) {
      /* Look for first character. */
      if (source.charAt(i) != first) {
        while (++i <= max && source.charAt(i) != first) ;
      }

      /* Found first character, now look at the rest of v2 */
      if (i <= max) {
        int j = i + 1;
        int end = j + targetCount - 1;
        for (int k = targetOffset + 1; j < end && source.charAt(j) ==
            target[k]; j++, k++)
          ;

        if (j == end) {
          /* Found whole string. */
          if (buffer != null)
            return buffer.position() + (i - sourceOffset);
          else
            return (i - sourceOffset);
        }
      }
    }
    return -1;
  }

  static CharBuffer copyString(String source) {
    // create a char buffer ...
    CharBuffer buffer = CharBuffer.allocate(source.length());

    // copy in original string ...
    buffer.put(source);

    return buffer;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

}

