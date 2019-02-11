/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment.scheduler;

import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.jetbrains.annotations.NotNull;

class LockAdapter {
    public static final int DEFAULT_RETRY_INTERVAL = 1;

    private static final int RETRY_INTERVAL = getInteger(
            "oak.segment.commit-lock-retry-interval", DEFAULT_RETRY_INTERVAL);

    @NotNull
    private static final Runnable NOOP = () -> { };

    @NotNull
    private final AtomicReference<Runnable> unlock = new AtomicReference<>(NOOP);

    @NotNull
    private final Semaphore semaphore;

    @NotNull
    private final Supplier<RecordId> headId;

    private final int retryInterval;

    private volatile int generation = Integer.MAX_VALUE;

    public LockAdapter(@NotNull Semaphore semaphore, @NotNull Supplier<RecordId> headId, int retryInterval) {
        this.semaphore = semaphore;
        this.headId = headId;
        this.retryInterval = retryInterval;
    }

    public LockAdapter(@NotNull Semaphore semaphore, @NotNull Supplier<RecordId> headId) {
        this(semaphore, headId, RETRY_INTERVAL);
    }

    public void lockAfterRefresh(Commit commit) throws InterruptedException {
        int commitGeneration = getFullGeneration(commit.refresh());
        checkInterrupted();
        while (!tryLock(commitGeneration, retryInterval, SECONDS)) {
            commitGeneration = getFullGeneration(commit.refresh());
            checkInterrupted();
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    private synchronized boolean tryLock(int generation, int time, @NotNull TimeUnit unit)
    throws InterruptedException {
        if (semaphore.tryAcquire(time, unit)) {
            this.generation = generation;
            unlock.set(this::release);
            return true;
        } else {
            if (getFullGeneration(headId.get()) > this.generation) {
                // compaction created a new generation while this lock was owned by a commit
                unlock();
            }
            return false;
        }
    }

    private void release() {
        this.generation = Integer.MAX_VALUE;
        semaphore.release();
    }

    public void unlock() {
        unlock.getAndSet(NOOP).run();
    }

    private static int getFullGeneration(@NotNull RecordId recordId) {
        return recordId.getSegment().getGcGeneration().getFullGeneration();
    }
}
