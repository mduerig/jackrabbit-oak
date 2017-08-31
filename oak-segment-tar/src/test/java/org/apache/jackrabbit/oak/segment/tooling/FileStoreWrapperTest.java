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

package org.apache.jackrabbit.oak.segment.tooling;

import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.size;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Lists.transform;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.readAttributes;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.tooling.filestore.RecordId.newRecordId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.io.PatternFilenameFilter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.tooling.filestore.IOMonitor;
import org.apache.jackrabbit.oak.tooling.filestore.JournalEntry;
import org.apache.jackrabbit.oak.tooling.filestore.Node;
import org.apache.jackrabbit.oak.tooling.filestore.Property;
import org.apache.jackrabbit.oak.tooling.filestore.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.Store;
import org.apache.jackrabbit.oak.tooling.filestore.Tar;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class FileStoreWrapperTest {

    @Nonnull
    private final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Nonnull
    private final TemporaryFileStoreWithToolAPI tempFileStore = new TemporaryFileStoreWithToolAPI(tempFolder);

    @Nonnull @Rule
    public RuleChain chain = RuleChain
            .outerRule(tempFolder)
            .around(tempFileStore);

    private FileStore fileStore;
    private Store toolAPI;

    @Before
    public void setup() {
        fileStore = tempFileStore.getFileStore();
        toolAPI = tempFileStore.getToolAPI();
    }

    @Test
    public void testTars() throws IOException {
        SegmentNodeState head = fileStore.getHead();
        SegmentNodeBuilder builder = head.builder();
        builder.setProperty("blob", new TestBlob(4048 * 4048));
        fileStore.getRevisions().setHead(head.getRecordId(), builder.getNodeState().getRecordId());
        fileStore.flush();

        File[] files = tempFileStore.getDirectory().listFiles(new PatternFilenameFilter(".*\\.tar"));
        assertNotNull(files);
        List<File> expectedTars = reverse(asList(files));

        Iterable<Tar> actualTars = toolAPI.tars();

        assertTrue(elementsEqual(transform(expectedTars, File::getName), transform(actualTars, Tar::name)));
        assertTrue(elementsEqual(transform(expectedTars, File::length), transform(actualTars, Tar::size)));
        assertTrue(elementsEqual(transform(expectedTars, FileStoreWrapperTest::creationTime), transform(actualTars, Tar::timestamp)));
    }

    private static long creationTime(File file) {
        try {
            Path path = Paths.get(file.toURI());
            return readAttributes(path, BasicFileAttributes.class)
                    .creationTime()
                    .to(MILLISECONDS);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testSegments() throws IOException, InvalidFileStoreVersionException {
        RecordId headId = fileStore.getHead().getRecordId();

        Optional<Segment> segment = toolAPI.segment(headId.asUUID());
        assertEquals(Optional.of(headId.asUUID()), segment.map(Segment::id));
    }

    @Test
    public void testJournalEntries() throws IOException {
        long before = currentTimeMillis();
        List<RecordId> expectedRevisions = newArrayList();
        for (int k = 0; k < 5; k++) {
            SegmentNodeState head = fileStore.getHead();
            expectedRevisions.add(0, head.getRecordId());
            SegmentNodeBuilder builder = head.builder();
            builder.setChildNode("n-" + k);
            fileStore.getRevisions().setHead(head.getRecordId(), builder.getNodeState().getRecordId());
            fileStore.flush();
        }
        expectedRevisions.add(0, fileStore.getHead().getRecordId());
        long after = currentTimeMillis();

        Iterable<JournalEntry> journal = toolAPI.journalEntries();
        assertEquals(
                transform(expectedRevisions, r -> newRecordId(r.asUUID(), r.getRecordNumber())),
                newArrayList(transform(journal, JournalEntry::id)));

        Iterable<JournalEntry> withoutInitial = limit(journal, size(journal) - 1);
        Iterable<Long> timeStamps = transform(withoutInitial, JournalEntry::timestamp);
        assertTrue(all(timeStamps, t -> (t >= before) && (t <= after)));
    }

    @Test
    public void testNode() {
        SegmentNodeState head = fileStore.getHead();
        RecordId headId = head.getRecordId();
        Node node = toolAPI.node(newRecordId(headId.asUUID(), headId.getRecordNumber()));
        assertEquals(Optional.of(head), toolAPI.cast(node, NodeState.class));
        assertEquals(new NodeWrapper(head), node);

        PropertyState expectedProperty = head.getChildNode("a").getChildNode("aa").getProperty("x");
        Property actualProperty = node.node("a").node("aa").property("x");
        assertNotNull(expectedProperty);
        assertEquals(Optional.of(expectedProperty), toolAPI.cast(actualProperty, PropertyState.class));
        assertEquals(new PropertyWrapper(expectedProperty), actualProperty);
    }

    @Test
    public void testCastToRecordId() {
        RecordId headId = fileStore.getHead().getRecordId();
        Optional<RecordId> recordId = toolAPI.cast(
                newRecordId(headId.asUUID(), headId.getRecordNumber()), RecordId.class);
        assertEquals(Optional.of(headId), recordId);
    }

    @Test
    public void testCastToSegmentId() {
        UUID headId = fileStore.getHead().getRecordId().asUUID();
        Optional<SegmentId> segmentId = toolAPI.cast(headId, SegmentId.class);
        assertEquals(Optional.of(headId), segmentId.map(SegmentId::asUUID));
    }

    @Test
    public void testIOMonitor() throws IOException {
        CountingIOMonitor ioMonitor = new CountingIOMonitor();

        try (Closeable __ = toolAPI.addIOMonitor(ioMonitor)) {
            writeNode("n1");
        }
        assertEquals(0, ioMonitor.getBeforeReadCount());
        assertEquals(0, ioMonitor.getAfterReadCount());
        assertEquals(1, ioMonitor.getBeforeWriteCount());
        assertEquals(1, ioMonitor.getAfterWriteCount());

        writeNode("n2");
        assertEquals(0, ioMonitor.getBeforeReadCount());
        assertEquals(0, ioMonitor.getAfterReadCount());
        assertEquals(1, ioMonitor.getBeforeWriteCount());
        assertEquals(1, ioMonitor.getAfterWriteCount());
    }

    private void writeNode(@Nonnull String name) throws IOException {
        SegmentNodeState head = fileStore.getHead();
        SegmentNodeBuilder builder = head.builder();
        builder.setChildNode(name);
        fileStore.getRevisions().setHead(head.getRecordId(), builder.getNodeState().getRecordId());
        fileStore.flush();
    }

    private static class CountingIOMonitor implements IOMonitor {
        private int beforeReadCount;
        private int afterReadCount;
        private int beforeWriteCount;
        private int afterWriteCount;

        public int getBeforeReadCount() {
            return beforeReadCount;
        }

        public int getAfterReadCount() {
            return afterReadCount;
        }

        public int getBeforeWriteCount() {
            return beforeWriteCount;
        }

        public int getAfterWriteCount() {
            return afterWriteCount;
        }

        @Override
        public void beforeSegmentRead(@Nonnull File file, long msb, long lsb, int length) {
            beforeReadCount++;
        }

        @Override
        public void afterSegmentRead(
                @Nonnull File file, long msb, long lsb, int length, long elapsed) {
            afterReadCount++;
        }

        @Override
        public void beforeSegmentWrite(@Nonnull File file, long msb, long lsb, int length) {
            beforeWriteCount++;
        }

        @Override
        public void afterSegmentWrite(
                @Nonnull File file, long msb, long lsb, int length, long elapsed) {
            afterWriteCount++;
        }
    }

}
