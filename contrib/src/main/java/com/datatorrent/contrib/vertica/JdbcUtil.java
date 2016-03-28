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

import java.sql.Types;

import com.datatorrent.common.util.Pair;

/**
 * Utility class to define common functions used for jdbc
 */
public class JdbcUtil
{
  public static String buildSelectSql(String tableName, String[] selectColumns, String[] whereColumns)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static String buildSelectSql(String tableName, String[] selectColumns, String[] whereColumns, Pair<String, String>[] orderByColumns, int limit)
  {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public static String buildInsertSql(String tableName, String[] columns)
  {
    StringBuilder sb = new StringBuilder(100);
    sb.setLength(0);
    StringBuilder valuePlaceHolders = new StringBuilder(100);

    for (String column : columns) {
      if (sb.length() == 0) {
        sb.append(column);
        valuePlaceHolders.append("?");
      }
      else {
        sb.append(",").append(column);
        valuePlaceHolders.append(",?");
      }
    }

    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(tableName);
    sql.append(" (").append(sb).append(") VALUES");
    sql.append(" (").append(valuePlaceHolders).append(")");

    return sql.toString();
  }

  public static String buildCountSql(String tableName, String[] columns)
  {
    StringBuilder sb = new StringBuilder(100);
    sb.setLength(0);

    StringBuilder sql = new StringBuilder("SELECT COUNT (*) FROM ");

    sql.append(tableName).append(" WHERE ");

    for (String column : columns) {
      if (sb.length() == 0) {
        sb.append(column).append("=? ");
      }
      else {
        sb.append("AND ").append(column).append("=? ");
      }
    }

    sql.append(sb);
    return sql.toString();
  }

  public static int getType(String typeString)
  {
    int type;
    if (typeString.trim().toUpperCase().contains("DATE")) {
      type = Types.DATE;
    }
    else if (typeString.trim().toUpperCase().contains("TIMESTAMP")) {
      type = Types.TIMESTAMP;
    }
    else if (typeString.trim().toUpperCase().contains("DECIMAL")) {
      type = Types.DECIMAL;
    }
    else if (typeString.trim().toUpperCase().contains("INTEGER")) {
      type = Types.INTEGER;
    }
    else if (typeString.trim().toUpperCase().contains("NUMERIC")) {
      type = Types.NUMERIC;
    }
    else if (typeString.trim().toUpperCase().contains("BYTEINT")) {
      type = Types.TINYINT;
    }
    else if (typeString.trim().toUpperCase().contains("SMALLINT")) {
      type = Types.SMALLINT;
    }
    else if (typeString.trim().toUpperCase().contains("BIGINT")) {
      type = Types.BIGINT;
    }
    else if (typeString.trim().toUpperCase().contains("VARCHAR")) {
      type = Types.VARCHAR;
    }
    else if (typeString.trim().toUpperCase().contains("CHAR")) {
      type = Types.CHAR;
    }
    else {
      type = Types.VARCHAR;
    }

    return type;
  }

}
