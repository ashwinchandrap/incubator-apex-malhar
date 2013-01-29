/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.math;

import com.malhartech.engine.TestSink;
import com.malhartech.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.malhartech.lib.math.MarginKeyVal}. <p>
 *
 */
public class MarginKeyValTest
{
  /**
   * Test node logic emits correct results.
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new MarginKeyVal<String, Integer>());
    testNodeProcessingSchema(new MarginKeyVal<String, Double>());
    testNodeProcessingSchema(new MarginKeyVal<String, Float>());
    testNodeProcessingSchema(new MarginKeyVal<String, Short>());
    testNodeProcessingSchema(new MarginKeyVal<String, Long>());
  }

  public void testNodeProcessingSchema(MarginKeyVal oper)
  {
    TestSink<KeyValPair<String, Number>> marginSink = new TestSink<KeyValPair<String, Number>>();

    oper.margin.setSink(marginSink);

    oper.beginWindow(0);
    oper.numerator.process(new KeyValPair("a", 2));
    oper.numerator.process(new KeyValPair("b", 20));
    oper.numerator.process(new KeyValPair("c", 1000));

    oper.denominator.process(new KeyValPair("a", 2));
    oper.denominator.process(new KeyValPair("b", 40));
    oper.denominator.process(new KeyValPair("c", 500));
    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 3, marginSink.collectedTuples.size());
    for (int i = 0; i < marginSink.collectedTuples.size(); i++) {
      if ("a".equals(marginSink.collectedTuples.get(i).getKey())) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(0), marginSink.collectedTuples.get(i).getValue().doubleValue());
      }
      if ("b".equals(marginSink.collectedTuples.get(i).getKey())) {
        Assert.assertEquals("emitted value for 'b' was ", new Double(0.5), marginSink.collectedTuples.get(i).getValue().doubleValue());
      }
      if ("c".equals(marginSink.collectedTuples.get(i).getKey())) {
        Assert.assertEquals("emitted value for 'c' was ", new Double(-1), marginSink.collectedTuples.get(i).getValue().doubleValue());
      }
    }
  }
}