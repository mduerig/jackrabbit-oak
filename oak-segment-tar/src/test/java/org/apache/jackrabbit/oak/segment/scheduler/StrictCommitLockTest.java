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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

public class StrictCommitLockTest {

    private static class LockFixture {
        private final StrictCommitLock lock = new StrictCommitLock(true);

        private final AtomicInteger owner = new AtomicInteger(-1);

        public void lock(int owner, int expectedOwner) throws InterruptedException {
            checkArgument(owner >= 0);
            lock.lock(mock(Commit.class));
            assertEquals("Lock owned by wrong owner", expectedOwner, this.owner.get());
            this.owner.set(owner);
        }

        public void lock(int owner) throws InterruptedException {
            checkArgument(owner >= 0);
            lock.lock(mock(Commit.class));
            assertEquals("Acquired already locked lock", -1, this.owner.get());
            this.owner.set(owner);
        }

        public boolean tryLock(int owner) {
            checkArgument(owner >= 0);
            if (lock.tryLock()) {
                assertEquals("Acquired already locked lock", -1, this.owner.get());
                this.owner.set(owner);
                return true;
            } else {
                return false;
            }
        }

        public void unlock() {
            this.owner.set(-1);
            lock.unlock();
        }

        public void assertLocked(int expectedOwner) {
            checkArgument(expectedOwner >= 0);
            int actualOwner = owner.get();
            assertTrue("Expected lock to be locked", lock.isLocked());
            assertTrue("Expected lock to be locked", actualOwner >= 0);
            assertEquals("Lock is locked by wrong owner", expectedOwner, owner.get());
        }

        public void assertUnlocked() {
            assertEquals("Expected lock to be unlocked", -1, owner.get());
        }
    }

    @Test
    public void unlocked() {
        LockFixture lockFixture = new LockFixture();
        lockFixture.assertUnlocked();
    }

    @Test
    public void unlockUnlocked() {
        LockFixture lockFixture = new LockFixture();
        lockFixture.unlock();
        lockFixture.assertUnlocked();
    }

    @Test
    public void lockUnlock() throws InterruptedException {
        LockFixture lockFixture = new LockFixture();

        lockFixture.lock(0);
        lockFixture.assertLocked(0);

        lockFixture.unlock();
        lockFixture.assertUnlocked();
    }

    @Test
    public void lockLocked() throws InterruptedException, ExecutionException {
        LockFixture lockFixture = new LockFixture();

        lockFixture.lock(0);
        lockFixture.assertLocked(0);

        FutureTask<Void> lockedBy1 = runAsync(
                () -> lockFixture.lock(1));
        lockFixture.assertLocked(0);

        lockFixture.unlock();
        lockedBy1.get();
        lockFixture.assertLocked(1);

        lockFixture.unlock();
        lockFixture.assertUnlocked();
    }

    @Test(expected = CancellationException.class)
    public void lockInterrupt() throws InterruptedException, ExecutionException {
        LockFixture lockFixture = new LockFixture();

        lockFixture.lock(0);
        lockFixture.assertLocked(0);

        FutureTask<Void> lockedBy1 = runAsync(
                () -> lockFixture.lock(1));
        lockFixture.assertLocked(0);

        lockedBy1.cancel(true);
        lockedBy1.get();
    }

    @Test
    public void noLockLoss() throws ExecutionException, InterruptedException {
        LockFixture lockFixture = new LockFixture();

        lockFixture.tryLock(0);
        lockFixture.assertLocked(0);

        FutureTask<Void> lockedBy1 = runAsync(
                () -> lockFixture.lock(1, 0));
        lockFixture.assertLocked(0);

        try {
            lockedBy1.get(2, SECONDS);
            fail("Expected to not get the lock");
        } catch (TimeoutException ignore) {
            lockFixture.unlock();
            lockFixture.assertUnlocked();
        }
    }

    private interface Thunk extends Callable<Void> {
        void run() throws Exception;

        @Override
        default Void call() throws Exception {
            run();
            return null;
        }
    }

    private static FutureTask<Void> runAsync(Thunk thunk) {
        return runAsync((Callable<Void>) thunk);
    }

    private static <T> FutureTask<T> runAsync(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
        return task;
    }

}
