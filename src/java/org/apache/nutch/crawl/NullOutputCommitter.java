package org.apache.nutch.crawl;

import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

import java.io.IOException;

public class NullOutputCommitter extends OutputCommitter {
  @Override
  public void abortTask(TaskAttemptContext arg0) throws IOException {}

  @Override
  public void cleanupJob(JobContext arg0) throws IOException {}

  @Override
  public void commitTask(TaskAttemptContext arg0) throws IOException {}

  @Override
  public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
    return false;
  }

  @Override
  public void setupJob(JobContext arg0) throws IOException {}

  @Override
  public void setupTask(TaskAttemptContext arg0) throws IOException {}
}