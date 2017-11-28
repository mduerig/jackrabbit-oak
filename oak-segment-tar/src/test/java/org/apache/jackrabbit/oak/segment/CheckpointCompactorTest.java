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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CheckpointCompactorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    private CheckpointCompactor compactor;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        compactor = createCompactor(fileStore);
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        addTestContent("cp1", nodeStore);
        String cp1 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp2", nodeStore);
        String cp2 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = compactor.compact(EMPTY_NODE, uncompacted1, EMPTY_NODE);
        assertNotNull(compacted1);
        assertFalse(uncompacted1 == compacted1);
        assertEquals(uncompacted1, compacted1);
        assertSameStableId(uncompacted1, compacted1);
        fileStore.getRevisions().setHead(uncompacted1.getRecordId(), compacted1.getRecordId());

        NodeState cp1Compacted = nodeStore.retrieve(cp1);
        NodeState cp2Compacted = nodeStore.retrieve(cp2);
        assertSameStableId(cp2Compacted, compacted1.getChildNode("root"));
        assertSameRecord(cp2Compacted, compacted1.getChildNode("root"));

        // Simulate a 2nd compaction cycle
        SegmentNodeState base2 = fileStore.getHead();
        addTestContent("cp3", nodeStore);
        String cp3 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp4", nodeStore);
        String cp4 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted2 = fileStore.getHead();
        SegmentNodeState compacted2 = compactor.compact(base2, uncompacted2, base2);
        assertNotNull(compacted2);
        assertFalse(uncompacted2 == compacted2);
        assertEquals(uncompacted2, compacted2);
        assertSameStableId(uncompacted2, compacted2);
        fileStore.getRevisions().setHead(uncompacted2.getRecordId(), compacted2.getRecordId());

        NodeState cp3Compacted = nodeStore.retrieve(cp3);
        NodeState cp4Compacted = nodeStore.retrieve(cp4);
        assertSameStableId(cp4Compacted, compacted2.getChildNode("root"));
        assertSameRecord(cp4Compacted, compacted2.getChildNode("root"));
        assertSameStableId(cp1Compacted, nodeStore.retrieve(cp1));
        assertSameRecord(cp1Compacted, nodeStore.retrieve(cp1));
        assertSameStableId(cp2Compacted, nodeStore.retrieve(cp2));
        assertSameRecord(cp2Compacted, nodeStore.retrieve(cp2));
    }

    private static void assertSameStableId(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have the same stable ids",
                ((SegmentNodeState) node1).getStableId(),
                ((SegmentNodeState) node2).getStableId());
    }

    private static void assertSameRecord(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have been deduplicated",
                ((SegmentNodeState) node1).getRecordId(),
                ((SegmentNodeState) node2).getRecordId());
    }

    @Nonnull
    private static CheckpointCompactor createCompactor(@Nonnull FileStore fileStore) {
        SegmentWriter writer = defaultSegmentWriterBuilder("c")
                .build(fileStore);

        return new CheckpointCompactor(
                fileStore.getReader(),
                writer,
                fileStore.getBlobStore(),
                Suppliers.ofInstance(false),
                GCNodeWriteMonitor.EMPTY);
    }

    private static void addTestContent(@Nonnull String parent, @Nonnull NodeStore nodeStore)
    throws CommitFailedException, IOException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder parentBuilder = rootBuilder.child(parent);
        parentBuilder.setChildNode("a").setChildNode("aa").setProperty("p", 42);
        parentBuilder.getChildNode("a").setChildNode("bb").setChildNode("bbb");
        parentBuilder.setChildNode("b").setProperty("bin", createBlob(nodeStore, 42));
        parentBuilder.setChildNode("c").setProperty(binaryPropertyFromBlob("bins", createBlobs(nodeStore, 42, 43, 44)));
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static List<Blob> createBlobs(NodeStore nodeStore, int... sizes) throws IOException {
        List<Blob> blobs = newArrayList();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }

}
