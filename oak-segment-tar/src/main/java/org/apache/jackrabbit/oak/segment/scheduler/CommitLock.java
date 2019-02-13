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

import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;

public interface CommitLock {

    /**
     * Acquires this lock , blocking until it becomes available, or the thread is
     * {@link Thread#interrupt interrupted}.
     * <p>
     * Implementations can define additional semantics depending of the value of
     * the {@code commit} argument.
     *
     * @param commit  the current commit
     * @throws InterruptedException if the current thread is interrupted
     */
    void lock(@NotNull Commit commit) throws InterruptedException;

    /**
     * Acquires the lock if it is not locked at the time of invocation.
     *
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     */
    boolean tryLock();

    /**
     * Acquires the lock if it becomes available within the given waiting time and the current
     * thread has not been {@linkplain Thread#interrupt interrupted}.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit the time unit of the {@code timeout} argument
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     * @throws InterruptedException if the current thread is interrupted
     */
    boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns the status of this lock.
     *
     * @return {@code true} if locked, {@code false} otherwise
     */
    boolean isLocked();

    /**
     * Unlock this lock. This method has no effect if no thread is holding this lock.
     * The calling thread does not have to owen this lock in order to be able to unlock
     * this lock.
     */
    void unlock();
}
