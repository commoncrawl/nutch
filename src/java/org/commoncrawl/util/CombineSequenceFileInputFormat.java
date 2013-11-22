package org.commoncrawl.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Input format that is a <code>CombineFileInputFormat</code>-equivalent for
 * <code>SequenceFileInputFormat</code>.
 *
 * @see org.apache.hadoop.mapred.lib.CombineFileInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CombineSequenceFileInputFormat<K,V>
    extends CombineFileInputFormat<K,V> {
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public RecordReader<K,V> getRecordReader(InputSplit split, JobConf conf,
                                           Reporter reporter) throws IOException {
    return new CombineFileRecordReader(conf, (CombineFileSplit)split, reporter,
        SequenceFileRecordReaderWrapper.class);
  }

  /**
   * A record reader that may be passed to <code>CombineFileRecordReader</code>
   * so that it can be used in a <code>CombineFileInputFormat</code>-equivalent
   * for <code>SequenceFileInputFormat</code>.
   *
   * @see CombineFileRecordReader
   * @see CombineFileInputFormat
   * @see SequenceFileInputFormat
   */
  private static class SequenceFileRecordReaderWrapper<K,V>
      extends CombineFileRecordReaderWrapper<K,V> {
    // this constructor signature is required by CombineFileRecordReader
    public SequenceFileRecordReaderWrapper(CombineFileSplit split,
                                           Configuration conf, Reporter reporter, Integer idx) throws IOException {
      super(new SequenceFileInputFormat<K,V>(), split, conf, reporter, idx);
    }
  }

  @Override
  protected List<FileStatus> listStatus(JobContext job)throws IOException {
    List<FileStatus> files = super.listStatus(job);
    int len = files.size();
    for(int i=0; i < len; ++i) {
      FileStatus file = files.get(i);
      if (file.isDirectory()) {     // it's a MapFile
        Path p = file.getPath();
        FileSystem fs = p.getFileSystem(job.getConfiguration());
        // use the data file
        files.set(i, fs.getFileStatus(new Path(p, MapFile.DATA_FILE_NAME)));
      }
    }
    return files;
  }


  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    FileStatus[] files = super.listStatus(job);
    for (int i = 0; i < files.length; i++) {
      FileStatus file = files[i];
      if (file.isDirectory()) {     // it's a MapFile
        Path dataFile = new Path(file.getPath(), MapFile.DATA_FILE_NAME);
        FileSystem fs = file.getPath().getFileSystem(job);
        // use the data file
        files[i] = fs.getFileStatus(dataFile);
      }
    }
    return files;
  }
}
