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
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentTestConstants;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileStoreIT {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Test
    public void testRestartAndGCWithoutMM() throws Exception {
        testRestartAndGC(false);
    }

    @Test
    public void testRestartAndGCWithMM() throws Exception {
        testRestartAndGC(true);
    }

    public void testRestartAndGC(boolean memoryMapping) throws Exception {
        FileStore store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(memoryMapping).build();
        store.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(memoryMapping).build();
        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        byte[] data = new byte[10 * 1024 * 1024];
        new Random().nextBytes(data);
        Blob blob = builder.createBlob(new ByteArrayInputStream(data));
        builder.setProperty("foo", blob);
        store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
        store.flush();
        store.getRevisions().setHead(store.getRevisions().getHead(), base.getRecordId());
        store.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(memoryMapping).build();
        store.fullGC();
        store.flush();
        store.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(memoryMapping).build();
        store.close();
    }

    @Test
    public void testRecovery() throws Exception {
        FileStore store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(false).build();
        store.flush();

        RandomAccessFile data0 = new RandomAccessFile(new File(getFileStoreFolder(), "data00000a.tar"), "r");
        long pos0 = data0.length();

        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        ArrayBasedBlob blob = new ArrayBasedBlob(new byte[SegmentTestConstants.MEDIUM_LIMIT]);
        builder.setProperty("blob", blob);
        builder.setProperty("step", "a");
        store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
        store.flush();
        long pos1 = data0.length();
        data0.close();

        base = store.getHead();
        builder = base.builder();
        builder.setProperty("step", "b");
        store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
        store.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(false).build();
        assertEquals("b", store.getHead().getString("step"));
        store.close();

        RandomAccessFile file = new RandomAccessFile(
                new File(getFileStoreFolder(), "data00000a.tar"), "rw");
        file.setLength(pos1);
        file.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(false).build();
        assertEquals("a", store.getHead().getString("step"));
        store.close();

        file = new RandomAccessFile(
                new File(getFileStoreFolder(), "data00000a.tar"), "rw");
        file.setLength(pos0);
        file.close();

        store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(false).build();
        assertFalse(store.getHead().hasProperty("step"));
        store.close();
    }

    @Test
    public void nonBlockingROStore() throws Exception {
        FileStore store = fileStoreBuilder(getFileStoreFolder()).withMaxFileSize(1).withMemoryMapping(false).build();
        store.flush(); // first 1kB
        SegmentNodeState base = store.getHead();
        SegmentNodeBuilder builder = base.builder();
        builder.setProperty("step", "a");
        store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
        store.flush(); // second 1kB

        ReadOnlyFileStore ro = null;
        try {
            ro = fileStoreBuilder(getFileStoreFolder()).buildReadOnly();
            assertEquals(store.getRevisions().getHead(), ro.getRevisions().getHead());
        } finally {
            if (ro != null) {
                ro.close();
            }
            store.close();
        }
    }

    @Test
    public void setRevisionTest() throws Exception {
        try (FileStore store = fileStoreBuilder(getFileStoreFolder()).build()) {
            RecordId id1 = store.getRevisions().getHead();
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setProperty("step", "a");
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
            RecordId id2 = store.getRevisions().getHead();
            store.flush();

            try (ReadOnlyFileStore roStore = fileStoreBuilder(getFileStoreFolder()).buildReadOnly()) {
                assertEquals(id2, roStore.getRevisions().getHead());

                roStore.setRevision(id1.toString());
                assertEquals(id1, roStore.getRevisions().getHead());

                roStore.setRevision(id2.toString());
                assertEquals(id2, roStore.getRevisions().getHead());
            }
        }
    }

    /**
     * This test case simulates adding revisions to a store, blocking journal
     * updates, adding more revisions followed by a unclean shutdown.
     */
    @Test
    public void blockedJournalUpdatesCauseSNFE()
    throws IOException, InvalidFileStoreVersionException, InterruptedException {
        try (FileStore rwStore = fileStoreBuilder(getFileStoreFolder()).build()) {

            // Block scheduled journal updates
            CountDownLatch blockJournalUpdates = new CountDownLatch(1);
            rwStore.fileStoreScheduler.scheduleOnce("block", 0, SECONDS, () ->
                awaitUninterruptibly(blockJournalUpdates));

            // Add some revisions
            Map<String, String> roots = newLinkedHashMap();
            for (int k = 0; k < 1000; k++) {
                roots.putIfAbsent(addNode(rwStore, "g" + k), "g" + k);
            }

            // Explicit journal update
            rwStore.flush();

            // Add more revisions
            for (int k = 0; k < 1000; k++) {
                roots.putIfAbsent(addNode(rwStore, "b" + k), "b" + k);
            }

            // Open the store again in read only mode and check all revisions.
            // This simulates accessing the store after an unclean shutdown.
            try (ReadOnlyFileStore roStore = fileStoreBuilder(getFileStoreFolder()).buildReadOnly()) {
                List<Entry<String, String>> goodRevisions = newArrayList();
                List<Entry<String, String>> badRevisions = newArrayList();
                for (Entry<String, String> revision : roots.entrySet()) {
                    roStore.setRevision(revision.getKey());
                    try {
                        checkNode(roStore.getHead());
                        goodRevisions.add(revision);
                    } catch (SegmentNotFoundException snfe) {
                        badRevisions.add(revision);
                    }
                }

                if (!badRevisions.isEmpty()) {
                    System.out.println("Good roots: " + goodRevisions);
                    System.out.println("Bad roots: " + badRevisions);
                }

                System.out.println("good/bad roots: " + goodRevisions.size() + "/" + badRevisions.size());
                assertTrue(badRevisions.isEmpty());
            }
            finally {
                blockJournalUpdates.countDown();
            }
        }
    }

    private static String addNode(FileStore store, String name) throws InterruptedException {
        Thread thread = new Thread(() -> {
            SegmentNodeState base = store.getHead();
            SegmentNodeBuilder builder = base.builder();
            builder.setChildNode(name);
            store.getRevisions().setHead(base.getRecordId(), builder.getNodeState().getRecordId());
        });
        thread.start();
        thread.join();

        return store.getRevisions().getHead().toString();
    }

    private static void checkNode(NodeState node) {
        for (ChildNodeEntry cne : node.getChildNodeEntries()) {
            checkNode(cne.getNodeState());
        }
    }

}
