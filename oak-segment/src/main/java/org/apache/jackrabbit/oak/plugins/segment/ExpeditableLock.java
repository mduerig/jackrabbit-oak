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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Sets.newConcurrentHashSet;
import static java.lang.Thread.currentThread;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;

/**
 * This is an implementation of a lock with similar properties than
 * {@link ReentrantLock} but with the additional option to expedite
 * entrance to the lock. That is, from all the threads waiting to
 * enter the lock the expedited ones will get preference. Other
 * threads will only enter the lock when there are no expedited
 * threads.
 * <p>
 * Apart from that {@code ExpeditedLock} is reentrant: a thread can
 * enter the lock multiple times but needs to leave it the same
 * number of times to release it.
 */
public class ExpeditableLock {
    private final Set<Thread> expedited = newConcurrentHashSet();
    private final Monitor monitor;
    private final Monitor.Guard expeditedEmpty;

    /**
     * Creates a expeditable lock with the given fairness policy.
     * @param fair whether this lock should be fair as opposed to not-fair but fast.
     */
    public ExpeditableLock(boolean fair) {
        monitor = new Monitor(fair);
        expeditedEmpty = new Guard(monitor) {
            @Override
            public boolean isSatisfied() {
                return expedited.isEmpty();
            }
        };
    }

    /**
     * Creates a expeditable lock with a non-fair fairness policy.
     */
    public ExpeditableLock() {
        this(false);
    }

    /**
     * Enter this lock. Blocks indefinitely.
     * @param expedite  whether to expedite entry to this lock or not.
     */
    public void lock(boolean expedite) {
        if (expedite) {
            expedited.add(currentThread());
            monitor.enter();
        } else {
            monitor.enterWhenUninterruptibly(expeditedEmpty);
        }
    }

    /**
     * Enters this lock if it is possible to do so immediately. Does not block.
     * @param expedite  whether to expedite entry to this lock or not.
     */
    public boolean tryLock(boolean expedite) {
        if (expedite) {
            expedited.add(currentThread());
            if (monitor.tryEnter()) {
                return true;
            } else {
                expedited.remove(currentThread());
                return false;
            }
        } else {
            return monitor.tryEnterIf(expeditedEmpty);
        }
    }

    /**
     * Enter this lock. Blocks no longer than the given time.
     * @param expedite  whether to expedite entry to this lock or not.
     */
    public boolean tryLock(boolean expedite, long time, TimeUnit unit) {
        if (expedite) {
            expedited.add(currentThread());
            if (monitor.enter(time, unit)) {
                return true;
            } else {
                expedited.remove(currentThread());
                return false;
            }
        } else {
            return monitor.enterWhenUninterruptibly(expeditedEmpty, time, unit);
        }
    }

    /**
     * Leave this lock.
     * @throws IllegalMonitorStateException  if the calling thread does not own the lock
     */
    public void unlock() {
        if (monitor.getOccupiedDepth() == 1) {
            expedited.remove(currentThread());
        }
        monitor.leave();
    }

    /**
     * Returns an estimate of the number of threads waiting to
     * acquire this lock.  The value is only an estimate because the number of
     * threads may change dynamically while this method traverses
     * internal data structures.  This method is designed for use in
     * monitoring of the system state, not for synchronization
     * control.
     * @return the estimated number of threads waiting for this lock
     */
    public int getQueueLength() {
        return monitor.getQueueLength();
    }

}
