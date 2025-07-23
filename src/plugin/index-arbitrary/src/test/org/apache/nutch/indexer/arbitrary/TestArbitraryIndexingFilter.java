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
package org.apache.nutch.indexer.arbitrary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that the index-arbitrary filter can add a new field with an arbitrary
 * value, supplement an existing field with an arbitrary value, and overwrite
 * an existing field with an arbitrary value where it takes the arbitrary value
 * from some POJO outside the normal Nutch codebase.
 * Further tests July 2025 to demonstrate conditionally setting values for
 * arbitrary fields based on existing crawl data at the time filter() is
 * called for indexing.
 * @author Joe Gilvary
 */

public class TestArbitraryIndexingFilter {

  Configuration conf;
  Inlinks inlinks;
  ParseImpl parse;
  CrawlDatum crawlDatum;
  Text url;
  ArbitraryIndexingFilter filter;
  NutchDocument doc;

  @Before
  public void setUp() throws Exception {
    parse = new ParseImpl();
    url = new Text("http://nutch.apache.org/index.html");
    crawlDatum = new CrawlDatum();
    inlinks = new Inlinks();
  }


  /**
   * Test adding field with arbitrary content from POJO
   * 
   * @throws Exception
   */
   @Test
   public void testAddingNewField() throws Exception {
     conf = NutchConfiguration.create();
     conf.set("index.arbitrary.function.count","1");
     conf.set("index.arbitrary.fieldName.0","foo");
     conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.Echo");
     conf.set("index.arbitrary.constructorArgs.0","Arbitrary text to add - bar");
     conf.set("index.arbitrary.methodName.0","getText");

     filter = new ArbitraryIndexingFilter();
     Assert.assertNotNull("No filter exists for testAddingNewField",filter);

     filter.setConf(conf);
     doc = new NutchDocument();
    
     try {
       filter.filter(doc, parse, url, crawlDatum, inlinks);
     } catch (Exception e) {
       e.printStackTrace();
       Assert.fail(e.getMessage());
     }

     Assert.assertNotNull(doc);
     Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
                        .isEmpty());
     Assert.assertTrue("test if doc has new field with arbitrary value", doc.getField("foo")
                       .getValues().contains("Arbitrary text to add - bar"));
   }

  /**
   * Test supplementing a doc field with arbitrary content from POJO
   * 
   * @throws Exception
   */
  @Test
  public void testSupplementExistingField() throws Exception {

    conf = NutchConfiguration.create();
    conf.set("index.arbitrary.function.count","2");
    conf.set("index.arbitrary.fieldName.0","foo");
    conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.Echo");
    conf.set("index.arbitrary.constructorArgs.0","Arbitrary text to add - bar");
    conf.set("index.arbitrary.methodName.0","getText");
    conf.set("index.arbitrary.fieldName.1","description");
    conf.set("index.arbitrary.className.1","org.apache.nutch.indexer.arbitrary.Multiplier");
    conf.set("index.arbitrary.constructorArgs.1","");
    conf.set("index.arbitrary.methodName.1","getProduct");
    conf.set("index.arbitrary.methodArgs.1","-1,3.14");

    filter = new ArbitraryIndexingFilter();
    Assert.assertNotNull("No filter exists for testSupplementExistingField", filter);

    filter.setConf(conf);
    
    doc = new NutchDocument();
    Assert.assertNotNull("doc doesn't exist", doc);

    doc.add("description","irrational");

    Assert.assertFalse("doc is empty", doc.getFieldNames().isEmpty());

    Assert.assertEquals("field description does not have exactly one value", 1,
                         doc.getField("description").getValues().size());
    
    Assert.assertTrue("field description does not have initial value 'irrational'",
                       doc.getField("description").getValues().contains("irrational"));
    
    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }

    Assert.assertTrue("doc doesn't have new field with arbitrary value",
                      doc.getField("foo").getValues()
                      .contains("Arbitrary text to add - bar"));

    Assert.assertEquals("field description does not have 2 values", 2,
                       doc.getField("description").getValues().size());

    Assert.assertTrue("field description original value gone", doc.getField("description")
                      .getValues().contains("irrational"));

    Assert.assertTrue("field description missing new value", doc.getField("description")
                     .getValues().contains("-3.14"));
  }


  /**
   * Test overwriting a doc field with arbitrary content from POJO
   * 
   * @throws Exception
   */
  @Test
  public void testOverwritingExistingField() throws Exception {
    conf = NutchConfiguration.create();
    conf.set("index.arbitrary.function.count","3");
    conf.set("index.arbitrary.fieldName.0","foo");
    conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.Echo");
    conf.set("index.arbitrary.constructorArgs.0","Arbitrary text to add - bar");
    conf.set("index.arbitrary.methodName.0","getText");
    conf.set("index.arbitrary.fieldName.1","description");
    conf.set("index.arbitrary.className.1","org.apache.nutch.indexer.arbitrary.Multiplier");
    conf.set("index.arbitrary.methodArgs.1","-1,3.14159265");
    conf.set("index.arbitrary.methodName.1","getProduct");
    conf.set("index.arbitrary.fieldName.2","philosopher");
    conf.set("index.arbitrary.className.2","org.apache.nutch.indexer.arbitrary.Echo");
    conf.set("index.arbitrary.constructorArgs.2","Popeye");
    conf.set("index.arbitrary.methodName.2","getText");
    conf.set("index.arbitrary.overwrite.2","true");
    
    filter = new ArbitraryIndexingFilter();
    Assert.assertNotNull("No filter exists for testOverwritingExistingField",filter);

    filter.setConf(conf);
    Assert.assertNotNull("conf does not exist",conf);
    
    doc = new NutchDocument();

    Assert.assertNotNull("doc does not exist",doc);

    doc.add("description","irrational");
    doc.add("philosopher","Socrates");

    Assert.assertEquals("field description does not have exactly one value", 1, doc.getField("description")
                        .getValues().size());

    Assert.assertEquals("field philosopher does not have exactly one value", 1, doc.getField("philosopher")
                        .getValues().size());

    Assert.assertTrue("field description does not have initial value 'irrational'", doc.getField("description")
                      .getValues().contains("irrational"));

    Assert.assertTrue("field philosopher does not have initial value 'Socrates'", doc.getField("philosopher")
                      .getValues().contains("Socrates"));
    
    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace(System.out);
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);

    Assert.assertEquals("field philosopher no longer has only one value", 1, doc.getField("philosopher")
                       .getValues().size());

    Assert.assertFalse("field philosopher's original value 'Socrates' NOT overwritten", doc.getField("philosopher")
                      .getValues().contains("Socrates"));

    Assert.assertTrue("field philosopher does not have new value 'Popeye'", doc.getField("philosopher")
                    .getValues().contains("Popeye"));
  }

  /**
   * Test processing a field after exception processing earlier field
   *
   * @throws Exception
   */
  @Test
  public void testProcessingFieldAfterException() throws Exception {
    conf = NutchConfiguration.create();
    conf.set("index.arbitrary.function.count","3");
    conf.set("index.arbitrary.fieldName.0","foo");
    conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.Echo");
    conf.set("index.arbitrary.constructorArgs.0","first added value");
    conf.set("index.arbitrary.methodName.0","getText");

    conf.set("index.arbitrary.fieldName.1","mangled");
    conf.set("index.arbitrary.className.1","java.lang.String");
    conf.set("index.arbitrary.constructorArgs.1","bar");
    conf.set("index.arbitrary.methodName.1","noExistingMethod");
    conf.set("index.arbitrary.methodArgs.1","100");
    conf.set("index.arbitrary.overwrite.1","true");

    conf.set("index.arbitrary.fieldName.2","philosopher");
    conf.set("index.arbitrary.className.2","org.apache.nutch.indexer.arbitrary.Echo");
    conf.set("index.arbitrary.constructorArgs.2","last added value");
    conf.set("index.arbitrary.methodName.2","getText");
    conf.set("index.arbitrary.overwrite.2","true");

    filter = new ArbitraryIndexingFilter();
    Assert.assertNotNull("No filter exists for testProcessingFieldAfterException",filter);

    filter.setConf(conf);
    Assert.assertNotNull("conf does not exist",conf);

    doc = new NutchDocument();

    Assert.assertNotNull("doc does not exist",doc);

    try {
      filter.filter(doc, parse, url, crawlDatum, inlinks);
    } catch (Exception e) {
      e.printStackTrace(System.out);
      Assert.fail(e.getMessage());
    }

    Assert.assertNotNull(doc);

    Assert.assertTrue("field foo does not have 'first added value'", doc.getField("foo")
                      .getValues().contains("first added value"));

    Assert.assertNull("field mangled has a value", doc.getField("mangled"));

    Assert.assertFalse("Value 'first added value' has leaked into field philospoher", doc.getField("philosopher")
                      .getValues().contains("first added value"));

    Assert.assertTrue("field philosopher does not have new value 'last added value'", doc.getField("philosopher")
                    .getValues().contains("last added value"));
  }


  /**
   * Test adding field with arbitrary content from POJO based on
   * calculations using info already fetched and parsed during crawl.
   *
   * @throws Exception
   */
   @Test
   public void testAddingNewCalculatedField() throws Exception {
     conf = NutchConfiguration.create();
     conf.set("index.arbitrary.function.count","1");
     conf.set("index.arbitrary.all.fields.access.0","true");
     conf.set("index.arbitrary.constructorArgs.0","");
     conf.set("index.arbitrary.fieldName.0","popularityBoost");
     conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.PopularityGauge");
     conf.set("index.arbitrary.methodName.0","getPopularityBoost");
     conf.set("index.arbitrary.overwrite.0","true");

     filter = new ArbitraryIndexingFilter();
     Assert.assertNotNull("No filter exists for testAddingCalculatedNewField",filter);

     filter.setConf(conf);
     doc = new NutchDocument();

     Double boostVal = Double.valueOf("1.0");
     doc.add("popularityBoost", boostVal);
     Assert.assertFalse("doc is empty", doc.getFieldNames().isEmpty());
     Assert.assertTrue("test if doc has new field with arbitrary value", doc.getField("popularityBoost")
                       .getValues().contains(boostVal));

     try {
       filter.filter(doc, parse, url, crawlDatum, inlinks);
     } catch (Exception e) {
       e.printStackTrace();
       Assert.fail(e.getMessage());
     }

     Assert.assertNotNull(doc);
     Assert.assertFalse("doc is empty", doc.getFieldNames().isEmpty());
     Assert.assertTrue("test if unfetched doc has nonzero value in popularityBoost", doc.getField("popularityBoost")
                       .getValues().contains(1.0));

     inlinks.add(new Inlink("https://www.TeamSauropod.com/BullyForBrontosaurus","dinosaur"));
     inlinks.add(new Inlink("https://github.com/apache","source code"));
     inlinks.add(new Inlink("https://BanDH.com","baseball"));

     crawlDatum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);

     try {
       filter.filter(doc, parse, url, crawlDatum, inlinks);
     } catch (Exception e) {
       e.printStackTrace();
       Assert.fail(e.getMessage());
     }

     Assert.assertTrue("test if successfully fetched doc has expected value in popularityBoost", doc.getField("popularityBoost")
                       .getValues().contains(2.0));
   }

  /**
   * Test simplest approach to updating POJOs to new signature
   * for constructor
   *
   * @throws Exception
   */
   @Test
   public void testUpdatingPOJOClass() throws Exception {
     conf = NutchConfiguration.create();
     conf.set("index.arbitrary.function.count","4");
     conf.set("index.arbitrary.fieldName.0","foo");
     conf.set("index.arbitrary.className.0","org.apache.nutch.indexer.arbitrary.Echo");
     conf.set("index.arbitrary.constructorArgs.0","Original Echo class added 'bar'");
     conf.set("index.arbitrary.methodName.0","getText");

     conf.set("index.arbitrary.fieldName.1","bogusSite");
     conf.set("index.arbitrary.className.1","org.apache.nutch.indexer.arbitrary.UpdatedEcho");
     conf.set("index.arbitrary.constructorArgs.1","https://www.updatedNutchPluginJunitTest.com");
     conf.set("index.arbitrary.methodName.1","getText");
     conf.set("index.arbitrary.all.fields.access.1","false");

     conf.set("index.arbitrary.fieldName.2","description");
     conf.set("index.arbitrary.className.2","org.apache.nutch.indexer.arbitrary.UpdatedMultiplier");
     conf.set("index.arbitrary.constructorArgs.2","");
     conf.set("index.arbitrary.methodName.2","getProduct");
     conf.set("index.arbitrary.methodArgs.2","-1,3.14");
     conf.set("index.arbitrary.all.fields.access.2","true");

     conf.set("index.arbitrary.fieldName.3","summary");
     conf.set("index.arbitrary.className.3","org.apache.nutch.indexer.arbitrary.UpdatedMultiplier");
     conf.set("index.arbitrary.constructorArgs.3","");
     conf.set("index.arbitrary.methodName.3","getProduct");
     conf.set("index.arbitrary.methodArgs.3","41,25");
     conf.set("index.arbitrary.all.fields.access.3","false");

     filter = new ArbitraryIndexingFilter();
     Assert.assertNotNull("No filter exists for testAddingNewField",filter);

     filter.setConf(conf);
     doc = new NutchDocument();

     try {
       filter.filter(doc, parse, url, crawlDatum, inlinks);
     } catch (Exception e) {
       e.printStackTrace();
       Assert.fail(e.getMessage());
     }

     Assert.assertNotNull(doc);
     Assert.assertFalse("test if doc is not empty", doc.getFieldNames()
                        .isEmpty());
     Assert.assertTrue("test if doc still has new field with arbitrary value running with new indexer", doc.getField("foo")
                       .getValues().contains("Original Echo class added 'bar'"));
     Assert.assertTrue("test if updated POJO created new field with arbitrary value", doc.getField("bogusSite")
                       .getValues().contains("https://www.updatedNutchPluginJunitTest.com"));
     Assert.assertTrue("test updated POJO set new value in existing field", doc.getField("description")
                       .getValues().contains("-3.14"));
     Assert.assertTrue("test POJO with both constructor styles supports old calls", doc.getField("summary")
			 .getValues().contains("1025.0"));
   }
}
