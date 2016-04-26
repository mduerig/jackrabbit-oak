/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.compaction;

// michid doc
public interface SegmentRevisionGC {
    String TYPE = "SegmentRevisionGarbageCollection";

    boolean isPausedCompaction();
    void setPausedCompaction(boolean pausedCompaction);

    /**
     * Get the compaction gain estimate threshold beyond which compaction should
     * run
     * @return gainThreshold
     */
    int getGainThreshold();

    /**
     * Set the compaction gain estimate threshold beyond which compaction should
     * run
     * @param gainThreshold
     */
    void setGainThreshold(int gainThreshold);

    int getMemoryThreshold();

    void setMemoryThreshold(int memory);

    /**
     * Get the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @return  retry count
     */
    int getRetryCount();

    /**
     * Set the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @param retryCount
     */
    void setRetryCount(int retryCount);

    /**
     * Get whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @return  {@code true} if force commit is on, {@code false} otherwise
     */
    boolean getForceAfterFail();

    /**
     * Set whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @param forceAfterFail
     */
    void setForceAfterFail(boolean forceAfterFail);

    int getLockWaitTime();

    void setLockWaitTime(int millis);

}
