/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.migrate;

import org.apache.iotdb.db.engine.migrate.MigrateLogReader;
import org.apache.iotdb.db.engine.migrate.MigrateLogWriter;
import org.apache.iotdb.db.engine.migrate.MigrateTask;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MigrateLogWriterReaderTest {

  private static String filePath = "logtest.test";
  private String sg1 = "root.MIGRATE_SG1";
  private String sg2 = "root.MIGRATE_SG1";
  private long startTime = 1672502400000L; // 2023-01-01
  private long ttl = 2000;
  private String targetDirPath = Paths.get("data", "separated").toString();
  List<MigrateLogWriter.MigrateLog> migrateLogs;
  MigrateTask task1;
  MigrateTask task2;

  @Before
  public void prepare() throws IllegalPathException {
    if (new File(filePath).exists()) {
      new File(filePath).delete();
    }
    task1 = new MigrateTask(120, new PartialPath(sg1), new File(targetDirPath), startTime, ttl);
    task2 = new MigrateTask(999, new PartialPath(sg1), new File(targetDirPath), startTime, ttl);
    migrateLogs.add(new MigrateLogWriter.MigrateLog(MigrateLogWriter.LogType.START, task1));
    migrateLogs.add(new MigrateLogWriter.MigrateLog(MigrateLogWriter.LogType.SET, task1));
    migrateLogs.add(new MigrateLogWriter.MigrateLog(MigrateLogWriter.LogType.UNSET, task2));
    migrateLogs.add(new MigrateLogWriter.MigrateLog(MigrateLogWriter.LogType.PAUSE, task2));
    migrateLogs.add(new MigrateLogWriter.MigrateLog(MigrateLogWriter.LogType.UNPAUSE, task2));
  }

  public void writeLog(MigrateLogWriter writer) throws IOException {
    writer.startMigrate(task1);
    writer.setMigrate(task1);
    writer.unsetMigrate(task2);
    writer.pauseMigrate(task2);
    writer.unpauseMigrate(task2);
  }

  public boolean logEquals(MigrateLogWriter.MigrateLog log1, MigrateLogWriter.MigrateLog log2) {
    if (log1.type != log2.type) {
      return false;
    }
    if (log1.index != log2.index) {
      return false;
    }
    if (log1.startTime != log2.startTime) {
      return false;
    }
    if (log1.ttl != log2.ttl) {
      return false;
    }
    if (!log1.storageGroup.getFullPath().equals(log2.storageGroup.getFullPath())) {
      return false;
    }
    if (!log1.targetDirPath.equals(log2.targetDirPath)) {
      return false;
    }

    return true;
  }

  @Test
  public void testWriteAndRead() throws Exception {
    MigrateLogWriter writer = new MigrateLogWriter(filePath);
    writeLog(writer);
    try {
      writer.close();
      MigrateLogReader reader = new MigrateLogReader(new File(filePath));
      List<MigrateLogWriter.MigrateLog> res = new ArrayList<>();
      while (reader.hasNext()) {
        res.add(reader.next());
      }
      for (int i = 0; i < migrateLogs.size(); i++) {
        assertTrue(logEquals(migrateLogs.get(i), res.get(i)));
      }
      reader.close();
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testReachEOF() throws Exception {
    try {
      // write normal data
      MigrateLogWriter writer = new MigrateLogWriter(filePath);
      try {
        writeLog(writer);
      } finally {
        writer.close();
      }
      long expectedLength = new File(filePath).length();

      // just write partial content
      try (FileOutputStream outputStream = new FileOutputStream(filePath, true);
          FileChannel channel = outputStream.getChannel()) {
        ByteBuffer logBuffer = ByteBuffer.allocate(4 * 30);
        for (int i = 0; i < 20; ++i) {
          logBuffer.putInt(Integer.MIN_VALUE);
        }
        logBuffer.flip();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(logBuffer.capacity());
        lengthBuffer.flip();

        channel.write(lengthBuffer);
        channel.write(logBuffer);
        channel.force(true);
      }

      // read & check
      MigrateLogReader reader = new MigrateLogReader(new File(filePath));
      try {
        List<MigrateLogWriter.MigrateLog> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < migrateLogs.size(); i++) {
          assertTrue(logEquals(migrateLogs.get(i), res.get(i)));
        }
      } finally {
        reader.close();
      }
      assertEquals(expectedLength, new File(filePath).length());
    } finally {
      new File(filePath).delete();
    }
  }

  @Test
  public void testTruncateBrokenLogs() throws Exception {
    try {
      // write normal data
      MigrateLogWriter writer = new MigrateLogWriter(filePath);
      try {
        writeLog(writer);
      } finally {
        writer.close();
      }
      long expectedLength = new File(filePath).length();

      // write broken data
      try (FileOutputStream outputStream = new FileOutputStream(filePath, true);
          FileChannel channel = outputStream.getChannel()) {
        ByteBuffer logBuffer = ByteBuffer.allocate(4 * 30);
        for (int i = 0; i < 30; ++i) {
          logBuffer.putInt(Integer.MIN_VALUE);
        }
        logBuffer.flip();

        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.putInt(logBuffer.limit());
        lengthBuffer.flip();

        CRC32 checkSummer = new CRC32();
        checkSummer.reset();
        checkSummer.update(logBuffer);
        ByteBuffer checkSumBuffer = ByteBuffer.allocate(8);
        checkSumBuffer.putLong(checkSummer.getValue());
        logBuffer.flip();
        checkSumBuffer.flip();

        channel.write(lengthBuffer);
        channel.write(logBuffer);
        channel.write(checkSumBuffer);
        channel.force(true);
      }

      // read & check
      MigrateLogReader reader = new MigrateLogReader(new File(filePath));
      try {
        List<MigrateLogWriter.MigrateLog> res = new ArrayList<>();
        while (reader.hasNext()) {
          res.add(reader.next());
        }
        for (int i = 0; i < migrateLogs.size(); i++) {
          assertTrue(logEquals(migrateLogs.get(i), res.get(i)));
        }
      } finally {
        reader.close();
      }
      assertEquals(expectedLength, new File(filePath).length());
    } finally {
      new File(filePath).delete();
    }
  }
}
