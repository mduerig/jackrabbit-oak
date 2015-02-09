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

import static java.io.File.createTempFile;

import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.oak.plugins.segment.Segment.Reader;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * michid document
 */
public class SegmentRewriteTest {

    private FileStore fileStore;

    @Before
    public void setUp() throws IOException {
        File directory = createTempFile(SegmentRewriteTest.class.getSimpleName(), "dir", new File("target"));
        directory.delete();
        directory.mkdir();

        fileStore = new FileStore(directory, 1);
        fileStore.flush();
    }

    @After
    public void tearDown() throws IOException {
        fileStore.cleanup();
    }

    @Test
    public void rewriteHead() {
        SegmentNodeState head = fileStore.getHead();
        RecordId headId = head.getRecordId();

        Segment s1 = fileStore.readSegment(headId.getSegmentId());
        headId.getSegmentId().rewrite();
        Segment s2 = fileStore.readSegment(headId.getSegmentId());

        System.out.println(s1);
        System.out.println(s2);

        Reader r1 = s1.getReader(headId);
        System.out.println(r1.readByte());
        System.out.println(r1.readByte());
        System.out.println(r1.readByte());

        Reader r2 = s2.getReader(headId);
        System.out.println(r2.readByte());
        System.out.println(r2.readByte());
        System.out.println(r2.readByte());
    }

}
