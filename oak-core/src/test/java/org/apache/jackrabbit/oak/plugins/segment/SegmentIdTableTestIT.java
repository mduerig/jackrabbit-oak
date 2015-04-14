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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Test;

public class SegmentIdTableTestIT {

    /**
     * OAK-2752
     */
    @Test
    public void endlessSearchLoop() {
        SegmentTracker tracker = new MemoryStore().getTracker();
        final SegmentIdTable tbl = new SegmentIdTable(tracker);

        List<SegmentId> refs = new ArrayList<SegmentId>();
        for (int i = 0; i < 1024; i++) {
            refs.add(tbl.getSegmentId(i, i % 64));
        }

        Callable<SegmentId> c = new Callable<SegmentId>() {

            @Override
            public SegmentId call() throws Exception {
                // (2,1) doesn't exist
                return tbl.getSegmentId(2, 1);
            }
        };
        Future<SegmentId> f = Executors.newSingleThreadExecutor().submit(c);
        SegmentId s = null;
        try {
            s = f.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertNotNull(s);
        Assert.assertEquals(2, s.getMostSignificantBits());
        Assert.assertEquals(1, s.getLeastSignificantBits());
    }
}
