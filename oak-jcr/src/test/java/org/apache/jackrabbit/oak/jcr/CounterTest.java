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

package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.junit.Before;
import org.junit.Test;

public class CounterTest extends AbstractRepositoryTest {
    private static final Random RND = new Random();

    public CounterTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        root.addNode("counter");
        session.save();
    }

    @Test
    public void counter() throws RepositoryException, ExecutionException, InterruptedException {
        final AtomicLong expectedCount = new AtomicLong(0);

        List<ListenableFutureTask<?>> tasks = Lists.newArrayList();
        for (int k = 0; k < 100; k ++) {
            tasks.add(updateCounter(k, RND.nextInt(21 - 10), expectedCount));
        }
        Futures.allAsList(tasks).get();

        Session session = createAdminSession();
        try {
            assertEquals(expectedCount.get(), session.getProperty("/counter/count").getLong());
        } finally {
            session.logout();
        }
    }

    private ListenableFutureTask<Void> updateCounter(final int id, final long delta, final AtomicLong expectedCount) {
        ListenableFutureTask<Void> task = ListenableFutureTask.create(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Session session = createAdminSession();
                try {
                    session.getNode("/counter").setProperty("delta-" + id, delta);
                    expectedCount.addAndGet(delta);
                    session.save();
                } finally {
                    session.logout();
                }
                return null;
            }
        });
        new Thread(task).start();
        return task;
    }

}
