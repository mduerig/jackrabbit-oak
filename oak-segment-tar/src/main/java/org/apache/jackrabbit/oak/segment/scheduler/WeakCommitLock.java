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

/**
 * This class implements a lock with some extra properties:
 * <ul>
 *     <li>No ownership: any thread can unlock this lock at anytime regardless if any thread and
 *     which thread is holding the lock.</li>
 *     <li>Loss of lock: any thread can lose the lock at anytime given it is holding it for
 *     too long and compaction completed in the meanwhile.</li>
 *     <li>Commit write ahead: pending changes in commits of threads trying to acquire this lock
 *     are written ahead before the lock is actually acquired.</li>
 * </ul>
 * Together those properties prevent long rewrite operation to take place while holding the lock
 * and thus blocking other commits. Such rewrite operations can be caused by compaction completing
 * making it necessary to rewrite pending commits to segments of the current garbage collection
 * generation.
 * <p>
 * See OAK-8014 for further details.
 */
class WeakCommitLock {

    /**
     * Default value for the {@code retryInterval} argument of
     * {@link WeakCommitLock#WeakCommitLock(boolean, Supplier, int)}
     */
    public static final int DEFAULT_RETRY_INTERVAL = 1;

    private static final int RETRY_INTERVAL = getInteger(
            "oak.segment.commit-lock-retry-interval", DEFAULT_RETRY_INTERVAL);

    @NotNull
    private static final Runnable NOOP = () -> { };

    @NotNull
    private final AtomicReference<Runnable> unlocker = new AtomicReference<>(NOOP);

    @NotNull
    private final Semaphore semaphore;

    @NotNull
    private final Supplier<RecordId> headId;

    private final int retryInterval;

    private volatile int generation = Integer.MAX_VALUE;

    /**
     * @param fair           {@code true} if this semaphore should make an effort to handle
     *                       grant the lock in a first come first serve order.
     * @param headId         a supplier returning the most recent head state when called
     * @param retryInterval  interval in seconds at which a commit of a thread trying to
     *                       acquire the lock will be written ahead.
     */
    public WeakCommitLock(boolean fair, @NotNull Supplier<RecordId> headId, int retryInterval) {
        this.semaphore = new Semaphore(1, fair);
        this.headId = headId;
        this.retryInterval = retryInterval;
    }

    /**
     * Equivalent to
     * <pre>
     *     new WeakCommitLock(fair, headId, RETRY_INTERVAL);
     * </pre>
     */
    public WeakCommitLock(boolean fair, @NotNull Supplier<RecordId> headId) {
        this(fair, headId, RETRY_INTERVAL);
    }

    /**
     * {@link Commit#writeAhead() Write ahead} pending changes in the current {@code commit}
     * and acquire this lock. Write ahead of pending changes also include rewriting this commit to
     * the current {@link org.apache.jackrabbit.oak.segment.file.tar.GCGeneration garbage collection generation}
     * after a successfully completed compaction. The write ahead operation is always completed
     * <em>before</em> the lock is actually acquired and will be reattempted periodically as
     * specified by the {@code retryInterval} that was passed to the
     * {@link WeakCommitLock#WeakCommitLock(boolean, Supplier, int) constructor} of this instance.
     * <p>
     * Trying to acquire this lock while it is held by another thread that acquired the lock through
     * this method can cause the other thread to lose the lock: this happens after the thread that
     * wishes to acquire the lock waited for at least the number of seconds specified in the
     * {@code retryInterval} that was passed to the
     * {@link WeakCommitLock#WeakCommitLock(boolean, Supplier, int) constructor} of this instance
     * <em>and</em> compaction completed successfully while the other thread was holding the lock.
     *
     * @param commit  the current commit
     * @throws InterruptedException if the current thread is interrupted
     */
    public void lock(@NotNull Commit commit) throws InterruptedException {
        checkInterrupted();
        int commitGeneration = getFullGeneration(commit.writeAhead());
        while (!tryLock(commitGeneration, retryInterval, SECONDS)) {
            checkInterrupted();
            commitGeneration = getFullGeneration(commit.writeAhead());
        }
    }

    /**
     * Acquires the lock if it is not locked at the time of invocation.
     * <p>
     * When the lock is acquired through this method it cannot be lost to other
     * threads trying to acquire the lock through {@link #lock(Commit)}.
     *
     * @return {@code true} if the lock was acquired, {@code false} otherwise
     */
    public boolean tryLock() {
        if (semaphore.tryAcquire()) {
            acquired(Integer.MAX_VALUE);
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
    public boolean tryLock(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
        if (semaphore.tryAcquire(timeout, unit)) {
            acquired(Integer.MAX_VALUE);
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

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }
    }

    private synchronized boolean tryLock(int generation, int time, @NotNull TimeUnit unit)
    throws InterruptedException {
        // This method needs to be synchronized to protect against data races between accesses
        // to this.generation in the if and else clause of the following condition.
        if (semaphore.tryAcquire(time, unit)) {
            acquired(generation);
            return true;
        } else {
            if (getFullGeneration(headId.get()) > this.generation) {
                // compaction created a new generation while this lock was held by another thread
                unlock();
            }
            return false;
        }
    }

    /* Callers must own the permit from {@code semaphore} */
    private void acquired(int generation) {
        // Setting this.generation *before* the unlocker ensures calls to release()
        // always see the correct generation
        this.generation = generation;
        unlocker.set(this::release);
    }

    private void release() {
        this.generation = Integer.MAX_VALUE;
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

    private static int getFullGeneration(@NotNull RecordId recordId) {
        return recordId.getSegment().getGcGeneration().getFullGeneration();
    }
}
