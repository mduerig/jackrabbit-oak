/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.tests;

import org.apache.jackrabbit.mk.scenarios.ConcurrentAddNodes1Commit;
import org.apache.jackrabbit.mk.testing.ConcurrentMicroKernelTestBase;
import org.junit.Test;

import com.cedarsoft.test.utils.CatchAllExceptionsRule;

/**
 * Test class for microkernel concurrent writing.All the nodes are added in a
 * single commit.
 */

public class MkConcurrentAddNodes1CommitTest extends
        ConcurrentMicroKernelTestBase {

    // nodes for each worker
    int nodesNumber = 100;

    /**
     * @Rule public CatchAllExceptionsRule catchAllExceptionsRule = new
     *       CatchAllExceptionsRule();
     **/
    @Test
    public void testConcurentWritingFlatStructure() throws InterruptedException {

        ConcurrentAddNodes1Commit.concurentWritingFlatStructure(mks, 3,
                nodesNumber, chronometer);
    }

    @Test
    public void testConcurentWritingPyramid1() throws InterruptedException {

        ConcurrentAddNodes1Commit.concurentWritingPyramid1(mks, 3, nodesNumber,
                chronometer);
    }

    @Test
    public void testConcurentWritingPyramid2() throws InterruptedException {

        ConcurrentAddNodes1Commit.concurentWritingPyramid2(mks, 3, nodesNumber,
                chronometer);
    }
}
