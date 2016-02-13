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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.After;
import org.junit.Test;

public class ExpeditableLockTest {
    private final ListeningExecutorService executor = listeningDecorator(newFixedThreadPool(3));
    private final ExpeditableLock lock = new ExpeditableLock();

    private ListenableFuture<Boolean>[] threads;

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void lock() throws ExecutionException, InterruptedException {
        prepare(2);
        try {
            threads[0] = lock(false, new Runnable() {
                @Override
                public void run() {
                    assertTrue("Expedited lock has precedence", threads[1].isDone());
                }
            });
            threads[1] = lock(true, new Runnable() {
                @Override
                public void run() {
                    assertFalse("Expedited lock has precedence", threads[0].isDone());
                }
            });
        } finally {
            run();
        }
        assertEquals(asList(true, true), allAsList(threads).get());
    }

    @Test
    public void tryLockTimed() throws ExecutionException, InterruptedException {
        prepare(2);
        try {
            threads[0] = tryLockWithTimeout(false, new Runnable() {
                @Override
                public void run() {
                    assertTrue("Expedited lock has precedence", threads[1].isDone());
                }
            });
            threads[1] = tryLockWithTimeout(true, new Runnable() {
                @Override
                public void run() {
                    assertFalse("Expedited lock has precedence", threads[0].isDone());
                }
            });
        } finally {
            run();
        }
        assertEquals(asList(true, true), allAsList(threads).get());
    }

    @Test
    public void tryLock() throws ExecutionException, InterruptedException {
        prepare(2);
        threads[0] = tryLock(new Runnable() {
            @Override
            public void run() {
                fail("Lock is occupied");
            }
        });
        threads[1] = tryLock(new Runnable() {
            @Override
            public void run() {
                fail("Lock is occupied");
            }
        });
        assertEquals(asList(false, false), allAsList(threads).get());
    }

    @Test
    public void tryLockTimeout() throws ExecutionException, InterruptedException {
        prepare(2);
        threads[0] = tryLockWithTimeout(false, new Runnable() {
            @Override
            public void run() {
                fail("Lock is occupied");
            }
        });
        threads[1] = tryLockWithTimeout(true, new Runnable() {
            @Override
            public void run() {
                fail("Lock is occupied");
            }
        });
        assertEquals(asList(false, false), allAsList(threads).get());
    }

    private void prepare(int threadCount) {
        checkState(threads == null);
        threads = new ListenableFuture[threadCount];
        lock.lock(false);
    }

    private void run() {
        checkState(threads != null);

        // Busy wait until above threads block on the lock
        while (lock.getQueueLength() != threads.length) {
            Thread.yield();
        }
        lock.unlock();
    }

    private ListenableFuture<Boolean> lock(final boolean expedite, final Runnable onLock) {
        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                lock.lock(expedite);
                try {
                    onLock.run();
                    return true;
                } finally {
                    lock.unlock();
                }
            }
        });
    }

    private ListenableFuture<Boolean> tryLock(final Runnable onLock) {
        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (lock.tryLock(false)) {
                    try {
                        onLock.run();
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    return false;
                }
            }
        });
    }
    private ListenableFuture<Boolean> tryLockWithTimeout(final boolean expedite, final Runnable onLock) {
        return executor.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (lock.tryLock(expedite, 1, SECONDS)) {
                    try {
                        onLock.run();
                        return true;
                    } finally {
                        lock.unlock();
                    }
                } else {
                    return false;
                }
            }
        });
    }
}
