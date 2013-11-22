package org.commoncrawl.util;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class NullOutputCommitter extends OutputCommitter {
  public void setupJob(JobContext jobContext) throws IOException {}

  public void setupTask(TaskAttemptContext taskContext)
      throws IOException {}

  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return false;
  }

  public void commitTask(TaskAttemptContext taskContext)
      throws IOException {}

  public void abortTask(TaskAttemptContext taskContext)
      throws IOException {}

}