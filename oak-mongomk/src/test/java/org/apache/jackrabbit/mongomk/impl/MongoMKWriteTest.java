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
 */
package org.apache.jackrabbit.mongomk.impl;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.util.MicroKernelInputStream;
import org.apache.jackrabbit.mongomk.BaseMongoMicroKernelTest;
import org.apache.jackrabbit.mongomk.MongoAssert;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.DB;

/**
 * Tests for {@code MongoMicroKernel#write(java.io.InputStream)}
 */
public class MongoMKWriteTest extends BaseMongoMicroKernelTest {

    // Override to set the right blob store.
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DB db = mongoConnection.getDB();
        dropCollections(db);

        MongoNodeStore nodeStore = new MongoNodeStore(db);
        MongoAssert.setNodeStore(nodeStore);
        BlobStore blobStore = new MongoBlobStore(db);
        mk = new MongoMicroKernel(mongoConnection, nodeStore, blobStore);
    }

    @Test
    public void small() throws Exception {
        write(1024);
    }

    @Test
    public void medium() throws Exception {
        write(1024 * 1024);
    }

    @Test
    public void large() throws Exception {
        write(20 * 1024 * 1024);
    }

    private void write(int blobLength) throws Exception {
        byte[] blob = createBlob(blobLength);
        String blobId = mk.write(new ByteArrayInputStream(blob));
        assertNotNull(blobId);

        byte[] readBlob = MicroKernelInputStream.readFully(mk, blobId);
        assertTrue(Arrays.equals(blob, readBlob));
    }

    private byte[] createBlob(int blobLength) {
        byte[] blob = new byte[blobLength];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte)i;
        }
        return blob;
    }
}