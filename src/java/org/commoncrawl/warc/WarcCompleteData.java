package org.commoncrawl.warc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.Content;


public class WarcCompleteData implements Writable {
  public Text url;
  public CrawlDatum datum;
  public Content content;

  public WarcCompleteData() {
    url = new Text();
    datum = new CrawlDatum();
    content = new Content();
  }

  public WarcCompleteData(Text url, CrawlDatum datum, Content content) {
    this.url = url;
    this.datum = datum;
    this.content = content;
  }

  public void readFields(DataInput in) throws IOException {
    url.readFields(in);
    if (in.readBoolean()) {
      datum.readFields(in);
    } else {
      datum = null;
    }
    if (in.readBoolean()) {
      content.readFields(in);
    } else {
      content = null;
    }
  }

  public void write(DataOutput out) throws IOException {
    url.write(out);
    if (datum != null) {
      out.writeBoolean(true);
      datum.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (content != null) {
      out.writeBoolean(true);
      content.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  public String toString() {
    return "url=" + url.toString() + ", datum=" + datum.toString();
  }
}