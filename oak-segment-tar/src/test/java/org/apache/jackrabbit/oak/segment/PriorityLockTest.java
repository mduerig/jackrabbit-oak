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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Thread.sleep;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PriorityLockTest {
    private final Random rnd = new Random();

    private ExecutorService executor;

    @Before
    public void setup() {
        executor = newFixedThreadPool(50);
    }

    @After
    public void tearDown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(1, SECONDS);
    }

    @Test
    public void testPriorityOrdering() throws InterruptedException, ExecutionException {
        PriorityLock lock = new PriorityLock();
        List<Future<Integer>> results = newArrayList();
        List<Integer> expectedOrder = newArrayList();
        List<Integer> actualOrder = newArrayList();
        for (int priority = 0; priority < 20; priority++) {
            expectedOrder.add(priority);
            results.add(executor.submit(new LockTask(lock, priority, actualOrder)));
        }

        for (Future<Integer> result : results) {
            result.get();
        }
        assertEquals("Tasks should execute in order of their priority",
                expectedOrder, actualOrder);
    }

    @Test
    public void tryLockUnsuccessfull() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        lock.lock(1);
        try {
            Future<Integer> tryLockTask = executor.submit(new TryLockTask(lock, 0));
            assertEquals("Lock should be busy already", -1, (int) tryLockTask.get());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void tryLockSuccessfull() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        Future<Integer> tryLockTask = executor.submit(new TryLockTask(lock, 0));
        assertEquals("Lock should not be busy", 0, (int) tryLockTask.get());
    }

    @Test
    public void raceTryDifferentPriorities() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        Future<Integer> tryLockTask0 = executor.submit(new TryLockTask(lock, 0));
        Future<Integer> tryLockTask1 = executor.submit(new TryLockTask(lock, 1));

        assertTrue("At least one task should get the lock",
            tryLockTask0.get() == 0 || tryLockTask1.get() == 1);
    }

    @Test
    public void raceTryEqualPriorities() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        Future<Integer> tryLockTask0 = executor.submit(new TryLockTask(lock, 0));
        Future<Integer> tryLockTask1 = executor.submit(new TryLockTask(lock, 0));

        assertTrue("At least one task should get the lock",
                tryLockTask0.get() == 0 || tryLockTask1.get() == 0);
    }

    @Test
    public void tryLockUnsuccessfullTimed() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        lock.lock(1);
        try {
            Future<Integer> tryLockTask = executor.submit(new TryLockTask(lock, 0, 1, SECONDS));
            assertEquals("Lock should be busy already", -1, (int) tryLockTask.get());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void reenterLock() throws InterruptedException {
        PriorityLock lock = new PriorityLock();
        lock.lock(1);
        lock.lock(1);
        assertTrue(lock.tryLock(2));
        lock.unlock();
        lock.unlock();
        lock.unlock();
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException ignore) {}
    }

    @Test
    public void tryLockSuccessfullTimed() throws ExecutionException, InterruptedException {
        final PriorityLock lock = new PriorityLock();
        lock.lock(1);
        Future<Integer> tryLockTask = executor.submit(new TryLockTask(lock, 0, 2, SECONDS));
        sleep(500);
        lock.unlock();
        assertEquals("Lock should not be busy", 0, (int) tryLockTask.get());
    }

    @Test
    public void raceTryDifferentPrioritiesTimed() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        Future<Integer> tryLockTask0 = executor.submit(new TryLockTask(lock, 0, 1, SECONDS));
        Future<Integer> tryLockTask1 = executor.submit(new TryLockTask(lock, 1, 1, SECONDS));

        assertEquals("Task 0 should get the lock", 0, (int) tryLockTask0.get());
        assertEquals("Task 1 should get the lock", 1, (int) tryLockTask1.get());
    }

    @Test
    public void raceTryEqualPrioritiesTimed() throws ExecutionException, InterruptedException {
        PriorityLock lock = new PriorityLock();
        Future<Integer> tryLockTask0 = executor.submit(new TryLockTask(lock, 0, 1, SECONDS));
        Future<Integer> tryLockTask1 = executor.submit(new TryLockTask(lock, 0, 1, SECONDS));

        assertEquals("Task 0 should get the lock", 0, (int) tryLockTask0.get());
        assertEquals("Task 1 should get the lock", 0, (int) tryLockTask1.get());
    }

    private class LockTask implements Callable<Integer> {
        private final PriorityLock lock;
        private final int priority;
        private final List<Integer> completions;

        LockTask(PriorityLock lock, int priority, List<Integer> completions) {
            this.lock = lock;
            this.priority = priority;
            this.completions = completions;
        }

        @Override
        public Integer call() throws Exception {
            lock.lock(priority);
            try {
                sleepUninterruptibly(rnd.nextInt(100), MILLISECONDS);
                completions.add(priority);
                return priority;
            } finally {
                lock.unlock();
            }
        }
    }

    private class TryLockTask implements Callable<Integer> {
        private final PriorityLock lock;
        private final int priority;
        private final int time;
        private final TimeUnit unit;

        TryLockTask(PriorityLock lock, int priority) {
            this(lock, priority, 0, null);
        }

        TryLockTask(PriorityLock lock, int priority, int time, TimeUnit unit) {
            this.lock = lock;
            this.priority = priority;
            this.time = time;
            this.unit = unit;
        }

        @Override
        public Integer call() throws Exception {
            if (unit == null) {
                return tryLock();
            } else {
                return tryLockTimed();
            }
        }

        private int tryLock() {
            if (lock.tryLock(priority)) {
                try {
                    sleepUninterruptibly(rnd.nextInt(100), MILLISECONDS);
                    return priority;
                } finally {
                    lock.unlock();
                }
            } else {
                return -1;
            }
        }

        private Integer tryLockTimed() throws InterruptedException {
            if (lock.tryLock(priority, time, unit)) {
                try {
                    sleepUninterruptibly(rnd.nextInt(100), MILLISECONDS);
                    return priority;
                } finally {
                    lock.unlock();
                }
            } else {
                return -1;
            }
        }
    }
}
