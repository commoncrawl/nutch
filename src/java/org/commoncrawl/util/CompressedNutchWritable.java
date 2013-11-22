package org.commoncrawl.util;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.util.GenericWritableConfigurable;

import java.io.DataOutput;
import java.io.IOException;

/**
 This is specifically for shuffling around the Content class. Since Hadoop compresses content for the mapper *after*
 it buffers it for sorting, we can significantly increase the effectiveness of our io.sort.mb buffer by compressing
 our Writables when we output them.
 */
public class CompressedNutchWritable extends CompressedGenericWritable {
  private static Class<? extends Writable>[] CLASSES = null;

  static {
    CLASSES = new Class[] {
        org.apache.hadoop.io.NullWritable.class,
        org.apache.hadoop.io.BooleanWritable.class,
        org.apache.hadoop.io.LongWritable.class,
        org.apache.hadoop.io.BytesWritable.class,
        org.apache.hadoop.io.FloatWritable.class,
        org.apache.hadoop.io.IntWritable.class,
        org.apache.hadoop.io.MapWritable.class,
        org.apache.hadoop.io.Text.class,
        org.apache.hadoop.io.MD5Hash.class,
        org.apache.nutch.crawl.CrawlDatum.class,
        org.apache.nutch.crawl.Inlink.class,
        org.apache.nutch.crawl.Inlinks.class,
        org.apache.nutch.indexer.NutchIndexAction.class,
        org.apache.nutch.metadata.Metadata.class,
        org.apache.nutch.parse.Outlink.class,
        org.apache.nutch.parse.ParseText.class,
        org.apache.nutch.parse.ParseData.class,
        org.apache.nutch.parse.ParseImpl.class,
        org.apache.nutch.parse.ParseStatus.class,
        org.apache.nutch.protocol.Content.class,
        org.apache.nutch.protocol.ProtocolStatus.class,
        org.apache.nutch.scoring.webgraph.LinkDatum.class,
    };
  }

  public CompressedNutchWritable() { }

  public CompressedNutchWritable(Writable instance) {
    set(instance);
  }

  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }

}