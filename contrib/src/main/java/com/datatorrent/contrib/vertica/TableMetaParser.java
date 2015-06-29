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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.common.collect.Maps;

import com.datatorrent.contrib.vertica.JdbcBatchInsertOperator.TableMeta;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * Parser to parse table meta from a given file
 */
public abstract class TableMetaParser
{
  public abstract Map<String, TableMeta> extractTableMeta(String fileName);

  public String getCountQuery(String table, String[] columns)
  {
    return JdbcUtil.buildCountSql(table, columns);
  }

  public String getInsertQuery(String table, String[] columns)
  {
    return JdbcUtil.buildInsertSql(table, columns);
  }

  /**
   * Parser to parse table meta from properties file
   */
  public static class TablePropertyFileParser extends TableMetaParser
  {
    @Override
    public Map<String, TableMeta> extractTableMeta(String fileName)
    {
      Properties properties = loadProperties(fileName);

      Map<String, TableMeta> tables = Maps.newHashMap();
      for (Entry<Object, Object> entry : properties.entrySet()) {
        String tablename = (String)entry.getKey();
        String colString = (String)entry.getValue();

        tables.put(tablename, getTable(tablename, colString, ":"));
      }

      return tables;
    }

    protected TableMeta getTable(String tablename, String colString, String delimiter)
    {
      TableMeta table = new TableMeta();
      table.tableName = tablename;
      String[] columns = colString.split(delimiter);

      table.countSql = getCountQuery(tablename, columns);
      table.insertSql = getInsertQuery(tablename, columns);

      return table;
    }

    protected Properties loadProperties(String fileName)
    {
      InputStream in = null;
      Properties properties;
      try {
        properties = new Properties();
        in = getClass().getResourceAsStream(fileName);
        properties.load(in);
        return properties;
      }
      catch (Exception ex) {
        DTThrowable.rethrow(ex);
      }
      finally {
        if (in != null) {
          try {
            in.close();
          }
          catch (IOException ex) {
            DTThrowable.rethrow(ex);
          }
        }
      }

      return null;
    }

  }

}
