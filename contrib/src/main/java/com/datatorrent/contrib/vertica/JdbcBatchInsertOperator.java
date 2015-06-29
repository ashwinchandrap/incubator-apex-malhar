/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.vertica;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.validation.constraints.NotNull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.db.jdbc.JdbcStore;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * Writes tuples from incoming file meta as batches to external relational database asynchronously using jdbc
 *
 * @param <FILEMETA>
 */
public class JdbcBatchInsertOperator<FILEMETA extends FileMeta> extends AbstractNonBlockingBatchWriter<FILEMETA, FILEMETA, Batch>
{
  protected transient FileSystem fs;
  @NotNull
  protected String filePath;
  private transient long fsRetryWaitMillis = 1000L;
  private transient int fsRetryAttempts = 3;
  private String rowDelimiter = "\\|";
  /*
   * holds partial new partial batches to be held temporarily generated batches are committed.
   */
  private Map<String, Batch> partialBatchesHolder = Maps.newHashMap();
  private JdbcStore store = new JdbcStore();
  /*
   * map from tableName to table meta
   */
  @NotNull
  private transient Map<String, TableMeta> tables;
  /*
   * File containing the mapping between table names and corresponding column names
   */
  @NotNull
  private String tableMappingFile;
  /*
   * parser implementation to parse the table mapping file
   */
  @NotNull
  private TableMetaParser parser;
  private String applicationId = null;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    try {
      fs = FileSystem.newInstance((new Path(filePath)).toUri(), new Configuration());
      if (applicationId == null) {
        applicationId = context.getValue(DAG.APPLICATION_ID);
        filePath = filePath + "/" + applicationId;
      }
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    store.connect();
    try {
      store.getConnection().setAutoCommit(false);
    }
    catch (SQLException ex) {
      throw new RuntimeException(ex);
    }

    tables = parser.extractTableMeta(tableMappingFile);
  }

  @Override
  protected FILEMETA convertInputTuple(FILEMETA input)
  {
    return input;
  }

  @Override
  protected List<Batch> generateBatches(FileMeta queueTuple)
  {
    jbioLogger.debug("queue tuple file meta = {}", queueTuple);
    List<String[]> newRows = fetchRowsFromFile(queueTuple);
    Batch existingPartialBatch = partialBatches.get(queueTuple.tableName);

    List<String[]> rows;
    if (existingPartialBatch != null) {
      rows = Lists.newArrayList();
      rows.addAll(existingPartialBatch.rows);
      rows.addAll(newRows);
    }
    else {
      rows = newRows;
    }
    List<Batch> insertBatches = Lists.newArrayList();

    int batchStart = 0;
    while (batchStart < rows.size()) {
      int batchEnd;
      if (rows.size() - batchStart < batchSize) {
        batchEnd = rows.size();
        Batch newPartialBatch = new Batch();
        newPartialBatch.tableName = queueTuple.tableName;
        newPartialBatch.rows = Lists.newArrayList();
        // for kryo, list returned by sublist does not have default constructor!
        newPartialBatch.rows.addAll(rows.subList(batchStart, batchEnd));
        partialBatchesHolder.put(queueTuple.tableName, newPartialBatch);
      }
      else {
        batchEnd = batchStart + batchSize;
        Batch batch = new Batch();
        batch.rows = Lists.newArrayList();
        batch.rows.addAll(rows.subList(batchStart, batchEnd));
        batch.tableName = queueTuple.tableName;

        insertBatches.add(batch);
      }
      batchStart = batchEnd;
    }

    return insertBatches;
  }

  /*
   * Used to update the partial batch to the last saved partial batch. Useful in case of recovery when partial batch is generated but
   * the batches before partial batch are not processed yet before operator failure.
   */
  @Override
  protected void processedCommittedData()
  {
    for (Entry<String, Batch> entry : partialBatchesHolder.entrySet()) {
      String string = entry.getKey();
      Batch batch = entry.getValue();

      partialBatches.put(string, batch);
    }
    partialBatchesHolder.clear();
  }

  @Override
  protected void executeBatch(Batch batch)
  {
    PreparedStatement stmt = null;
    Savepoint savepoint = null;
    try {
      TableMeta table = tables.get(batch.tableName);
      String insertSql = table.insertSql;
      savepoint = store.getConnection().setSavepoint();
      stmt = store.getConnection().prepareStatement(insertSql);

      jbioLogger.debug("prepared statement = {}", stmt);

      for (String[] values : batch.rows) {
        for (int i = 0; i < values.length; i++) {
          if (values[i].isEmpty()) {
            stmt.setNull(i + 1, table.types[i]);
          }
          else {
            stmt.setObject(i + 1, values[i], table.types[i]);
          }
        }

        stmt.addBatch();
      }

      stmt.executeBatch();
      store.getConnection().commit();
    }
    catch (BatchUpdateException batchEx) {
      jbioLogger.error("batch update exception: ", batchEx);
      try {
        store.getConnection().rollback(savepoint);
      }
      catch (SQLException sqlEx) {
        throw new RuntimeException(sqlEx);
      }

      throw new RuntimeException(batchEx);
    }
    catch (SQLException ex) {
      jbioLogger.error("sql exception: ", ex);
      try {
        store.getConnection().rollback(savepoint);
      }
      catch (SQLException sqlEx) {
        throw new RuntimeException(sqlEx);
      }
      throw new RuntimeException(ex);
    }
    catch (Exception ex) {
      jbioLogger.error("exception: ", ex);
      DTThrowable.rethrow(ex);
    }
    finally {
      try {
        if (stmt != null) {
          stmt.close();
        }
      }
      catch (SQLException ex) {
        jbioLogger.error("sql exception: ", ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Check to see if the first tuple of the batch is already inserted in the database
   *
   * @param batch
   */
  @Override
  protected boolean ensureBatchNotExecuted(Batch batch)
  {
    try {
      TableMeta tableMeta = tables.get(batch.tableName);
      PreparedStatement stmt = store.getConnection().prepareStatement(tableMeta.countSql);
      String[] row = batch.rows.get(0);

      jbioLogger.debug("looking up if tuple exists for table: {} and tuple: {} sql: {}", batch.tableName, Arrays.asList(row), tableMeta.countSql);

      for (int i = 0; i < row.length; i++) {
        jbioLogger.debug("setting value {} at index {}", row[i], i);
        stmt.setObject(i + 1, row[i]);
      }

      ResultSet resultSet = stmt.executeQuery();
      return resultSet.first();
    }
    catch (SQLException ex) {
      DTThrowable.rethrow(ex);
    }

    return false;
  }

  protected List<String[]> fetchRowsFromFile(FileMeta fileMeta)
  {
    List<String[]> rows;

    int retryNum = fsRetryAttempts;

    while (true) {
      try {
        rows = attempFetchRows(fileMeta);
        return rows;
      }
      catch (IOException ioEx) {
        /*
         * wait for some time and retry again as upstream operator might be recovering the file in case of failure failure
         */
        jbioLogger.error("exception while reading file {} will retry in {} ms \n exception: ", fileMeta.fileName, fsRetryWaitMillis, ioEx);
        if (retryNum-- > 0) {
          try {
            Thread.sleep(fsRetryWaitMillis);
          }
          catch (InterruptedException ex1) {
            DTThrowable.rethrow(ex1);
          }
        }
        else {
          jbioLogger.error("no more retries, giving up reading the file! {}: ", fileMeta);
          return null;
        }
      }
      catch (Exception ex) {
        jbioLogger.error("exception: ", ex);
      }
    }
  }

  private List<String[]> attempFetchRows(FileMeta fileMeta) throws IOException
  {
    BufferedReader bufferedReader = null;
    try {
      Path path = new Path(filePath + File.separator + fileMeta.fileName);
      jbioLogger.debug("path = {}", path);
      FSDataInputStream inputStream = fs.open(path);
      inputStream.seek(fileMeta.offset);

      bufferedReader = new BufferedReader(new InputStreamReader(inputStream), 1024 * 1024);

      List<String[]> rows = Lists.newArrayList();
      jbioLogger.debug("reading {} rows from file {}", fileMeta.numLines, path);
      for (int lineNum = 0; lineNum < fileMeta.numLines; lineNum++) {
        String line = bufferedReader.readLine();
        String[] row = line.split(rowDelimiter, -1);
        rows.add(row);
      }

      jbioLogger.debug("all rows added...row list size = {}", rows.size());
      return rows;
    }
    finally {
      if (bufferedReader != null) {
        bufferedReader.close();
      }
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();

    if (store.isConnected()) {
      store.disconnect();
    }

    if (fs != null) {
      try {
        fs.close();
      }
      catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  public long getFsRetryWaitMillis()
  {
    return fsRetryWaitMillis;
  }

  public void setFsRetryWaitMillis(long fsRetryWaitMillis)
  {
    this.fsRetryWaitMillis = fsRetryWaitMillis;
  }

  public int getFsRetryAttempts()
  {
    return fsRetryAttempts;
  }

  public void setFsRetryAttempts(int fsRetryAttempts)
  {
    this.fsRetryAttempts = fsRetryAttempts;
  }

  public String getDelimiter()
  {
    return rowDelimiter;
  }

  public void setDelimiter(String delimiter)
  {
    this.rowDelimiter = delimiter;
  }

  public TableMetaParser getParser()
  {
    return parser;
  }

  public void setParser(TableMetaParser parser)
  {
    this.parser = parser;
  }

  public String getTableMappingFile()
  {
    return tableMappingFile;
  }

  public void setTableMappingFile(String tableMappingFile)
  {
    this.tableMappingFile = tableMappingFile;
  }

  public static class TableMeta
  {
    public String tableName;
    public String insertSql;
    public String countSql;
    public String[] columns;
    public int[] types;
  }

  private static final Logger jbioLogger = LoggerFactory.getLogger(JdbcBatchInsertOperator.class);
}
