/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.dimensions;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.google.common.collect.Maps;
import javax.validation.constraints.NotNull;

import java.util.Map;

/**
 * This operator performs dimensions computation on a map.
 * @displayName Dimension Computation
 * @category Statistics
 * @tags event, dimension, aggregation, computation
 */
public class GenericDimensionsComputationSingleSchemaMap extends GenericDimensionsComputationSingleSchema<Map<String, Object>>
{
  @NotNull
  private Map<String, String> keyNameAliases = Maps.newHashMap();
  @NotNull
  private Map<String, String> valueNameAliases = Maps.newHashMap();

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    inputEvent = new InputEvent(
            new EventKey(0,
                         0,
                         0,
                         new GPOMutable(this.configurationSchema.getKeyDescriptorWithTime())),
            new GPOMutable(this.configurationSchema.getInputValuesDescriptor()));
  }

  @Override
  public void convert(InputEvent inputEvent, Map<String, Object> event)
  {
    populateGPO(inputEvent.getKeys(),
                event,
                keyNameAliases);

    populateGPO(inputEvent.getAggregates(),
                event,
                valueNameAliases);
  }

  private void populateGPO(GPOMutable gpoMutable,
                           Map<String, Object> event,
                           Map<String, String> aliasMap)
  {
    FieldsDescriptor fd = gpoMutable.getFieldDescriptor();

    for(int index = 0;
        index < fd.getFieldList().size();
        index++) {
      String field = fd.getFieldList().get(index);
      gpoMutable.setField(field, event.get(getMapAlias(aliasMap,
                                                     field)));
    }
  }

  private String getMapAlias(Map<String, String> map,
                             String name)
  {
    String aliasName = map.get(name);

    if(aliasName == null) {
      aliasName = name;
    }

    return aliasName;
  }

  /**
   * Gets the keyNameAliases.
   * @return The keyNameAliases.
   */
  public Map<String, String> getKeyNameAliases()
  {
    return keyNameAliases;
  }

  /**
   * Sets a map from key names as defined in the {@link DimensionalConfigurationSchema} to the
   * corresponding key names in input maps.
   * @param keyNameAliases The keyNameAliases to set.
   */
  public void setKeyNameAliases(Map<String, String> keyNameAliases)
  {
    this.keyNameAliases = keyNameAliases;
  }

  /**
   * Gets the valueNameAliases.
   * @return The valueNameAliases.
   */
  public Map<String, String> getValueNameAliases()
  {
    return valueNameAliases;
  }

  /**
   * Sets a map from value names as defined in the {@link DimensionalConfigurationSchema} to the
   * corresponding value names in input maps.
   * @param valueNameAliases The valueNameAliases to set.
   */
  public void setValueNameAliases(Map<String, String> valueNameAliases)
  {
    this.valueNameAliases = valueNameAliases;
  }
}
