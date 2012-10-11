/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.Component;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.math.ArithmeticSum;
import com.malhartech.lib.testbench.DevNullCounter;
import com.malhartech.lib.testbench.LoadGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class ApplicationLoadGenerator implements ApplicationFactory
{
  private static final boolean inline = true;


  public Operator getLoadGenerator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadGenerator.class);
    int numchars = 1024;
    char[] chararray = new char[numchars + 1];
    for (int i = 0; i < numchars; i++) {
      chararray[i] = 'a';
    }
    chararray[numchars] = '\0';
    String key = new String(chararray);
    oper.setProperty(LoadGenerator.KEY_KEYS, key);
    oper.setProperty(LoadGenerator.KEY_STRING_SCHEMA, "false");
    oper.setProperty(LoadGenerator.KEY_TUPLES_BLAST, "1000");
    oper.setProperty(LoadGenerator.ROLLING_WINDOW_COUNT, "10");
    oper.setProperty("spinMillis", "2");
    int i = 10 * 1024 * 1024;
    String ival = Integer.toString(i);
    oper.setProperty("bufferCapacity", ival);
    return oper;
  }

  public Operator getDevNull(String name, DAG b)
  {
    return b.addOperator(name, DevNullCounter.class);
  }
  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG b = new DAG(conf);

    Operator lgen = getLoadGenerator("lgen", b);
    Operator devnull = getDevNull("devnull", b);
    b.addStream("lgen2devnull", lgen.getOutput(LoadGenerator.OPORT_DATA), devnull.getInput(DevNullCounter.IPORT_DATA)).setInline(inline);
    return b;
  }
}
