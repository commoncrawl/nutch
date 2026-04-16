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
package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for {@link SitemapInjector}: points the injector at a local
 * {@code file://} sitemap, runs the full two-step MapReduce pipeline, then
 * reads the resulting CrawlDb and asserts the expected URLs (including
 * hreflang alternates) are present.
 */
public class TestSitemapInjector {

    private Configuration conf;
    private FileSystem fs;
    private final static Path testdir = new Path("build/test/sitemap-inject-test");
    private Path crawldbPath;
    private Path urlPath;
    private String sitemapUrl;

    @BeforeEach
    public void setUp() throws Exception {
        Configurator.setLevel(
                "org.apache.hadoop.mapred", org.apache.logging.log4j.Level.DEBUG);

        conf = CrawlDBTestUtil.createContext().getConfiguration();

        conf.set("plugin.folders", new File("build/plugins").getAbsolutePath());
        conf.set("plugin.includes",
                "protocol-file|urlfilter-regex|index-basic");
        conf.setInt("http.time.limit", 120);
        conf.setBoolean("mime.type.magic", false);
        conf.setBoolean("db.injector.sitemap.check-cross-submits", false);
        conf.setInt("mapreduce.mapper.multithreadedmapper.threads", 1);
        conf.set("http.filter.ipaddress.exclude", "");

        conf.set("urlfilter.regex.rules", "+.");
//        conf.set("urlfilter.regex.file",
//                new File("src/testresources/regex-urlfilter-test.txt").getAbsoluteFile().toURI().toString());

        urlPath = new Path(testdir, "urls");
        crawldbPath = new Path(testdir, "crawldb");
        fs = FileSystem.get(conf);
        fs.delete(testdir, true);
    }

    @AfterEach
    public void tearDown() throws IOException {
        fs.delete(testdir, true);
    }

    @Test
    public void injectsUrlsFromLocalSitemapKPMG() throws Exception {
        sitemapUrl = resolveFixture("sitemaps/sitemap.example.1.xml");

        List<String> seeds = new ArrayList<>();
        seeds.add(sitemapUrl);
        CrawlDBTestUtil.generateSeedList(fs, urlPath, seeds);

        SitemapInjector sitemapInjector = new SitemapInjector();
        sitemapInjector.setConf(conf);
        sitemapInjector.inject(crawldbPath, urlPath);

        List<String> injected = readCrawldb();

        assertFalse(injected.isEmpty(),
                "SitemapInjector produced an empty CrawlDb");

        // Primary <loc> URL from sitemap.example.1.xml
        assertTrue(
                injected.contains("https://kpmg.com/tr/tr/home/misc/accessibility.html"),
                "Primary <loc> URL missing from CrawlDb");

        // hreflang alternate from the same <url> block — exercises the
        // sitemap-localized-links extraction path.
        assertTrue(
                injected.contains("https://kpmg.com/de/de/home/misc/accessibility.html"),
                "hreflang alternate missing from CrawlDb (localized-links extraction failed)");

        assertThat(injected.size(), is(3156));
    }


    @Test
    public void injectsUrlsFromLocalSitemapOTHER() throws Exception {
        sitemapUrl = resolveFixture("sitemaps/sitemap.example.2.xml");

        List<String> seeds = new ArrayList<>();
        seeds.add(sitemapUrl);
        CrawlDBTestUtil.generateSeedList(fs, urlPath, seeds);

        SitemapInjector sitemapInjector = new SitemapInjector();
        sitemapInjector.setConf(conf);
        sitemapInjector.inject(crawldbPath, urlPath);

        List<String> injected = readCrawldb();

        assertFalse(injected.isEmpty(),
                "SitemapInjector produced an empty CrawlDb");

        assertThat(injected.size(), is(1732));
    }

    /**
     * Resolve a fixture path either from {@code build/test/data} (where
     * {@code ant test-core} copies {@code src/testresources}) or directly from
     * {@code src/testresources} when running from an IDE.
     */
    private String resolveFixture(String relative) {
        String testData = System.getProperty("test.build.data");
        if (testData != null) {
            File f = new File(testData, relative);
            if (f.exists()) {
                return f.toURI().toString();
            }
        }
        File src = new File("src/testresources", relative);
        if (!src.exists()) {
            src = new File("../src/testresources", relative);
        }
        return src.getAbsoluteFile().toURI().toString();
    }

    private List<String> readCrawldb() throws IOException {
        Path dbfile = new Path(crawldbPath,
                CrawlDb.CURRENT_NAME + "/part-r-00000/data");
        Option rFile = SequenceFile.Reader.file(dbfile);
        @SuppressWarnings("resource")
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, rFile);
        List<String> read = new ArrayList<>();
        try {
            while (true) {
                Text key = new Text();
                CrawlDatum value = new CrawlDatum();
                if (!reader.next(key, value)) {
                    break;
                }
                read.add(key.toString());
            }
        } finally {
            reader.close();
        }
        Collections.sort(read);
        return read;
    }
}
