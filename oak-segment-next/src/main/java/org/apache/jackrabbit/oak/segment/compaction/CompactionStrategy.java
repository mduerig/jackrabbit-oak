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
public class CompactionStrategy {
    public static final CompactionStrategy DEFAULT = new CompactionStrategy();

    /**
     * Default value for {@link #isPaused()}
     */
    public static final boolean PAUSE_DEFAULT = false;

    /**
     * Default value for {@link #getMemoryThreshold()}
     */
    public static final byte MEMORY_THRESHOLD_DEFAULT = 5;

    /**
     * Default value for {@link #getGainThreshold()}
     */
    public static final byte GAIN_THRESHOLD_DEFAULT = 10;

    /**
     * Default value for {@link #getRetryCount()}
     */
    public static final int RETRY_COUNT_DEFAULT = 5;

    /**
     * Default value for {@link #getForceAfterFail()}
     */
    public static final boolean FORCE_AFTER_FAIL_DEFAULT = false;

    /**
     * Default value for {@link #getLockWaitTime()}
     */
    public static final int LOCK_WAIT_TIME_DEFAULT = 60000;

    private boolean paused = PAUSE_DEFAULT;

    private int memoryThreshold = MEMORY_THRESHOLD_DEFAULT;

    /**
     * Compaction gain estimate threshold beyond which compaction should run
     */
    private int gainThreshold = GAIN_THRESHOLD_DEFAULT;

    private int retryCount = RETRY_COUNT_DEFAULT;

    private boolean forceAfterFail = FORCE_AFTER_FAIL_DEFAULT;

    private int lockWaitTime = LOCK_WAIT_TIME_DEFAULT;

    public CompactionStrategy(boolean paused, int memoryThreshold, int gainThreshold,
                              int retryCount, boolean forceAfterFail, int lockWaitTime) {
        this.paused = paused;
        this.memoryThreshold = memoryThreshold;
        this.gainThreshold = gainThreshold;
        this.retryCount = retryCount;
        this.forceAfterFail = forceAfterFail;
        this.lockWaitTime = lockWaitTime;
    }

    public CompactionStrategy() {
        this(PAUSE_DEFAULT, MEMORY_THRESHOLD_DEFAULT, GAIN_THRESHOLD_DEFAULT,
                RETRY_COUNT_DEFAULT, FORCE_AFTER_FAIL_DEFAULT, LOCK_WAIT_TIME_DEFAULT);
    }

    public boolean isPaused() {
        return paused;
    }

    public CompactionStrategy setPaused(boolean paused) {
        this.paused = paused;
        return this;
    }

    public int getMemoryThreshold() {
        return memoryThreshold;
    }

    public CompactionStrategy setMemoryThreshold(int memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
        return this;
    }

    /**
     * Get the compaction gain estimate threshold beyond which compaction should
     * run
     * @return gainThreshold
     */
    public int getGainThreshold() {
        return gainThreshold;
    }

    /**
     * Set the compaction gain estimate threshold beyond which compaction should
     * run
     * @param gainThreshold
     */
    public CompactionStrategy setGainThreshold(int gainThreshold) {
        this.gainThreshold = gainThreshold;
        return this;
    }

    /**
     * Get the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @return  retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Set the number of tries to compact concurrent commits on top of already
     * compacted commits
     * @param retryCount
     */
    public CompactionStrategy setRetryCount(int retryCount) {
        this.retryCount = retryCount;
        return this;
    }

    /**
     * Get whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @return  {@code true} if force commit is on, {@code false} otherwise
     */
    public boolean getForceAfterFail() {
        return forceAfterFail;
    }

    /**
     * Set whether or not to force compact concurrent commits on top of already
     * compacted commits after the maximum number of retries has been reached.
     * Force committing tries to exclusively write lock the node store.
     * @param forceAfterFail
     */
    public CompactionStrategy setForceAfterFail(boolean forceAfterFail) {
        this.forceAfterFail = forceAfterFail;
        return this;
    }

    public int getLockWaitTime() {
        return lockWaitTime;
    }

    public CompactionStrategy setLockWaitTime(int lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "paused=" + paused +
                ", memoryThreshold=" + memoryThreshold +
                ", gainThreshold=" + gainThreshold +
                ", retryCount=" + retryCount +
                ", forceAfterFail=" + forceAfterFail +
                ", lockWaitTime=" + lockWaitTime + '}';
    }

    /**
     * Check if the approximate repository size is getting too big compared with
     * the available space on disk.
     *
     * @param repositoryDiskSpace Approximate size of the disk space occupied by
     *                            the repository.
     * @param availableDiskSpace  Currently available disk space.
     * @return {@code true} if the available disk space is considered enough for
     * normal repository operations.
     */
    public boolean isDiskSpaceSufficient(long repositoryDiskSpace, long availableDiskSpace) {
        return availableDiskSpace > 0.25 * repositoryDiskSpace;
    }

}
