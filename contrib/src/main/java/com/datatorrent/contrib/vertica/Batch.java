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

import java.util.ArrayList;
import java.util.List;

/**
 * Holds a batch of tuples in memory for the given table
 */
public class Batch
{
  public String tableName;
  public int tableId;
  public ArrayList<String[]> rows;

  @Override
  public String toString()
  {
    return "Batch{" + "table=" + tableName + ", tableId=" + tableId + ", rows=" + getRowsAsString() + '}';
  }

  private String getRowsAsString()
  {
    if (rows == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(100);

    builder.append("{");
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      if (rowIdx != 0) {
        builder.append(", ");
      }

      builder.append("Row ").append(rowIdx).append(":[");

      String[] row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < row.length; colIdx++) {
        if (colIdx != 0) {
          builder.append(", ");
        }
        builder.append(row[colIdx]);
      }
      builder.append("]");
    }
    builder.append("}");
    return builder.toString();
  }

  public List<String> getTokenSeparatedRowList(String token)
  {
    List<String> rowList = new ArrayList<String>();

    StringBuilder rowStr = new StringBuilder(100);
    for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
      String[] row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < row.length; colIdx++) {
        if (colIdx != 0) {
          rowStr.append(token);
        }
        rowStr.append(row[colIdx]);
      }
      rowList.add(rowStr.toString());
      rowStr.setLength(0);
    }
    return rowList;
  }

  @Override
  public int hashCode()
  {
    int hash = 3;
    hash = 83 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
    return hash;
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Batch other = (Batch)obj;
    if ((this.tableName == null) ? (other.tableName != null) : !this.tableName.equals(other.tableName)) {
      return false;
    }
    return true;
  }

}
