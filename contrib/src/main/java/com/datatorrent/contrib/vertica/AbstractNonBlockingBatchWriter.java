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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import com.datatorrent.lib.io.fs.AbstractReconciler;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.common.util.NameableThreadFactory;

/**
 * This operator is used when writing batches to the external system is slower than the generation of the batch
 *
 * @param <INPUT> input type
 * @param <QUEUETUPLE> tuple enqueued each window to be processed after window is committed
 * @param <BATCH> batch that needs to be written out
 */
public abstract class AbstractNonBlockingBatchWriter<INPUT, QUEUETUPLE, BATCH> extends AbstractReconciler<INPUT, QUEUETUPLE>
{
  private transient ExecutorService batchExecute;
  /*
   * upto three batches will be available in memory for external writer
   */
  private BlockingQueue<BATCH> batchQueue = Queues.newLinkedBlockingQueue(3);
  /*
   * map from table to partial batches, used to cache spillover batches which have fewer than batchSize number of rows;
   */
  protected Map<String, BATCH> partialBatches = Maps.newHashMap();
  protected int batchSize = 10000;
  /*
   * used to finalize partial batch when no tulpes are seen for defined number of windows
   */
  protected int batchFinalizeWindowCount = 10;
  protected boolean tupleInWindow = false;
  protected int noTupleWindowCount = 0;
  protected boolean unsureLastExecutedBatch = true;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    unsureLastExecutedBatch = true;
    batchExecute = Executors.newSingleThreadExecutor(new NameableThreadFactory("BatchExecuteHelper"));
    batchExecute.submit(batchExecuteHandler());

  }

  @Override
  protected final void processTuple(INPUT input)
  {
    enqueueForProcessing(convertInputTuple(input));

    /*
     * used to track if atleast one tuple was received in the window
     */
    if (!tupleInWindow) {
      tupleInWindow = true;
    }
  }

  /**
   * Batch execute thread used to write batch to external system.
   * This is a separate thread to ensure that external I/O is not blocked by any of the batch load I/O
   *
   * By using blocking queue, it is also ensured that the queue is not populated with more than
   * the set number of batches when the batch writing is slower than batch generation.
   *
   * @return
   */
  private Runnable batchExecuteHandler()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          while (execute) {
            while (batchQueue.isEmpty()) {
              Thread.sleep(spinningTime);
            }

            BATCH batch = batchQueue.peek();

            if (unsureLastExecutedBatch) {
              if (ensureBatchNotExecuted(batch)) {
                executeBatch(batch);
                unsureLastExecutedBatch = false;
              }
            }
            else {
              executeBatch(batch);
            }

            batchQueue.remove();
          }
        }
        catch (Throwable e) {
          cause.set(e);
          execute = false;
        }
      }

    };
  }

  @Override
  protected void processCommittedData(QUEUETUPLE queueInput)
  {
    List<BATCH> batches = generateBatches(queueInput);
    try {
      for (BATCH batch : batches) {
        batchQueue.put(batch); // blocks if queue is full
      }
    }
    catch (InterruptedException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
    if (tupleInWindow) {
      tupleInWindow = false;
      noTupleWindowCount = 0;
    }
    else {
      noTupleWindowCount++;
    }

    // finalize partial batches
    if (noTupleWindowCount >= batchFinalizeWindowCount && getQueueSize() == 0) {
      List<BATCH> batches = retreivePartialBatches();
      try {
        for (BATCH batch : batches) {
          batchQueue.put(batch); // blocks if queue is full
        }
      }
      catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }

      noTupleWindowCount = 0;
    }
  }

  protected List<BATCH> retreivePartialBatches()
  {
    List<BATCH> batches = Lists.newArrayList();
    for (Entry<String, BATCH> entry : partialBatches.entrySet()) {
      BATCH batch = entry.getValue();
      batches.add(batch);
    }

    return batches;
  }

  protected abstract QUEUETUPLE convertInputTuple(INPUT input);

  protected abstract List<BATCH> generateBatches(QUEUETUPLE queueTuple);

  protected abstract void executeBatch(BATCH batch);

  /**
   * Used to check with external system after recovery if the batch has previously been processed or not
   *
   * @param batch
   * @return true is batch is not executed
   */
  protected abstract boolean ensureBatchNotExecuted(BATCH batch);

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchFinalizeWindowCount()
  {
    return batchFinalizeWindowCount;
  }

  public void setBatchFinalizeWindowCount(int batchFinalizeWindowCount)
  {
    this.batchFinalizeWindowCount = batchFinalizeWindowCount;
  }

}
