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

package org.apache.jackrabbit.oak.segment;

import static java.lang.Integer.compare;
import static java.lang.Thread.currentThread;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;

/**
 * This is an implementation of a lock with similar properties than
 * {@link ReentrantLock} but with the additional option to prioritise
 * entrance to the lock. That is, from all the threads waiting to
 * enter the lock the once with the highest priority will get preference.
 * Threads with lower priorities will only enter the lock once there are
 * no threads with higher priorities waiting to enter the lock.
 * The priority is specified via an extra int argument to the respective
 * lock methods. Lower numbers stand for higher priorities.
 * <p>
 * Apart from that {@code PriorityLock} is reentrant: a thread can
 * enter the lock multiple times but needs to leave it the same
 * number of times to release it.
 */
public class PriorityLock {
    private final Queue<Entry> entries = new PriorityBlockingQueue<>();
    private final Monitor monitor = new Monitor();
    private volatile Entry holder;

    private static class Entry implements Comparable<Entry> {
        final Thread thread;
        final int priority;

        Entry(Thread thread, int priority) {
            this.thread = thread;
            this.priority = priority;
        }

        @Override
        public int compareTo(Entry e) {
            return compare(priority, e.priority);
        }

        @Override
        public String toString() {
            return "Entry{thread=" + thread + ", priority=" + priority + '}';
        }
    }

    /**
     * Enters this lock if it is possible to do so immediately. Does not block.
     * @param priority  priority for the entry to this lock
     */
    public boolean tryLock(int priority) {
        if (monitor.isOccupiedByCurrentThread()) {
            monitor.enter();
            return true;
        }
        // Synchronize to ensure that at least one of  two concurrent callers
        // gets the lock.
        synchronized (this) {
            final Entry entry = new Entry(currentThread(), priority);
            entries.add(entry);
            if (monitor.tryEnterIf(isMaxPriority(entry))) {
                holder = entry;
                return true;
            } else {
                entries.remove(entry);
                return false;
            }
        }
    }

    /**
     * Enter this lock. Blocks no longer than the given time.
     * @param priority  priority for the entry to this lock
     */
    public boolean tryLock(int priority, long timeout, @Nonnull TimeUnit unit)
    throws InterruptedException {
        if (monitor.isOccupiedByCurrentThread()) {
            monitor.enter();
            return true;
        }
        final Entry entry = new Entry(currentThread(), priority);
        entries.add(entry);
        if (monitor.enterWhen(isMaxPriority(entry), timeout, unit)) {
            holder = entry;
            return true;
        } else {
            entries.remove(entry);
            return false;
        }
    }

    /**
     * Enter this lock. Blocks indefinitely.
     * @param priority  priority for the entry to this lock
     */
    public void lock(int priority) throws InterruptedException {
        if (monitor.isOccupiedByCurrentThread()) {
            monitor.enter();
            return;
        }
        final Entry entry = new Entry(currentThread(), priority);
        entries.add(entry);
        monitor.enterWhen(isMaxPriority(entry));
        holder = entry;
    }

    /**
     * Leave this lock.
     * @throws IllegalMonitorStateException  if the calling thread does not own the lock
     */
    public void unlock() {
        if (monitor.getOccupiedDepth() == 1) {
            entries.remove(holder);
            holder = null;
        }
        monitor.leave();
    }

    @Nonnull
    private Guard isMaxPriority(final Entry entry) {
        return new Guard(PriorityLock.this.monitor) {
            @Override
            public boolean isSatisfied() {
                return entries.peek() == entry;
            }
        };
    }

}
