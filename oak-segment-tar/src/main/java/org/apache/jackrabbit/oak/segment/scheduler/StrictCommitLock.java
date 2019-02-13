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

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;

class StrictCommitLock {

    @NotNull
    private final Semaphore semaphore;

    @NotNull
    private static final Runnable NOOP = () -> { };

    @NotNull
    private final AtomicReference<Runnable> unlocker = new AtomicReference<>(NOOP);

    /**
     * @param fair           {@code true} if this semaphore should make an effort to handle
     *                       grant the lock in a first come first serve order.
     */
    public StrictCommitLock(boolean fair) {
        this.semaphore = new Semaphore(1, fair);
    }

    /**
     * Acquires this lock , blocking until it becomes available, or the thread is
     * {@link Thread#interrupt interrupted}.
     *
     * @throws InterruptedException if the current thread is interrupted
     */
    public void lock(@NotNull Commit commit) throws InterruptedException {
        semaphore.acquire();
        acquired();
    }

    /**
     * Acquires the lock if it is not locked at the time of invocation.
     *
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     */
    public boolean tryLock() {
        if (semaphore.tryAcquire()) {
            acquired();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Acquires the lock if it becomes available within the given waiting time and the current
     * thread has not been {@linkplain Thread#interrupt interrupted}.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     * @throws InterruptedException if the current thread is interrupted
     */
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        if (semaphore.tryAcquire(timeout, unit)) {
            acquired();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the status of this lock.
     *
     * @return {@code true} if locked, {@code false} otherwise
     */
    public boolean isLocked() {
        return semaphore.availablePermits() < 1;
    }

    /* Callers must own the permit from {@code semaphore} */
    private void acquired() {
        unlocker.set(this::release);
    }

    private void release() {
        semaphore.release();
    }

    /**
     * Unlock this lock. This method has no effect if no thread is holding this lock.
     * The calling thread does not have to owen this lock in order to be able to unlock
     * this lock.
     */
    public void unlock() {
        // Looping unlock through the atomic object unlocker object reference ensures
        // the semaphore backing this lock is only ever release once per call to lock()
        unlocker.getAndSet(NOOP).run();
    }
}
