/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.benchmark;

import com.datatorrent.contrib.couchbase.AbstractCouchBaseInputOperator;
import com.datatorrent.contrib.couchbase.CouchBaseWindowStore;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>CouchBaseInputOperator class.</p>
 *
 * @since 2.0.0
 */
public class CouchBaseInputOperator extends AbstractCouchBaseInputOperator<String>
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseWindowStore.class);
  @Override
  public String getTuple(Object object)
  {
    if(object!=null)
    return object.toString();
    else{
     logger.info("Object returned is null");
     return "null";
    }
  }

  @Override
  public ArrayList<String> getKeys()
  {
    ArrayList<String> keys = new ArrayList<String>();
    keys.add("Key0");
    keys.add("Key10");
    keys.add("Key100");
    keys.add("Key110");
    keys.add("Key120");
    keys.add("Key130");
    keys.add("Key140");
    keys.add("Key150");
    return keys;
  }

}
