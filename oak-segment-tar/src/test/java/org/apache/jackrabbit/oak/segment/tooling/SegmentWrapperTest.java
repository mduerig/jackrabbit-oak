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

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.tooling.TypeMap.toOakType;
import static org.apache.jackrabbit.oak.tooling.filestore.RecordId.newRecordId;
import static org.apache.jackrabbit.oak.tooling.filestore.Segment.Type.DATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.tooling.filestore.Record;
import org.apache.jackrabbit.oak.tooling.filestore.RecordId;
import org.apache.jackrabbit.oak.tooling.filestore.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.SegmentMetaData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class SegmentWrapperTest {
    @Nonnull
    private final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Nonnull
    private final TemporaryFileStoreWithToolAPI tempFileStore = new TemporaryFileStoreWithToolAPI(tempFolder);

    @Nonnull @Rule
    public RuleChain chain = RuleChain
            .outerRule(tempFolder)
            .around(tempFileStore);

    private org.apache.jackrabbit.oak.segment.Segment segment;
    private Segment segmentWrapper;

    @Before
    public void setup() {
        FileStore fileStore = tempFileStore.getFileStore();
        segment = fileStore.getHead().getRecordId().getSegment();
        segmentWrapper = new SegmentWrapper(segment);
    }

    @Test
    public void testId() {
        assertEquals(segment.getSegmentId().asUUID(), segmentWrapper.id());
    }

    @Test
    public void testSize() {
        assertEquals(segment.size(), segmentWrapper.size());
    }

    @Test
    public void testType() {
        assertEquals(DATA, segmentWrapper.type());
    }

    @Test
    public void testReferences() {
        List<UUID> expectedReferences = newArrayList();
        for (int k = 0; k < segment.getReferencedSegmentIdCount(); k++) {
            expectedReferences.add(segment.getReferencedSegmentId(k));
        }

        assertFalse(expectedReferences.isEmpty());
        assertTrue(elementsEqual(expectedReferences, segmentWrapper.references()));
    }

    @Test
    public void testRecords() {
        List<RecordId> expectedRecordIds = newArrayList();
        List<RecordType> expectedRecordTypes = newArrayList();
        segment.forEachRecord((number, type, offset) -> {
            expectedRecordIds.add(newRecordId(segment.getSegmentId().asUUID(), offset));
            expectedRecordTypes.add(type);
        });

        assertFalse(expectedRecordIds.isEmpty());
        assertTrue(elementsEqual(expectedRecordIds, transform(segmentWrapper.records(), Record::id)));
        assertTrue(elementsEqual(expectedRecordTypes, transform(segmentWrapper.records(), t -> toOakType(t.type()))));
    }

    @Test
    public void metaData() {
        SegmentMetaData metaData = segmentWrapper.metaData();
        assertEquals(segment.getSegmentVersion(), SegmentVersion.fromByte((byte) metaData.version()));

        GCGeneration gcGeneration = segment.getGcGeneration();
        assertEquals(gcGeneration.getGeneration(), metaData.generation());
        assertEquals(gcGeneration.getFullGeneration(), metaData.fullGeneration());
        assertEquals(gcGeneration.isCompacted(), metaData.compacted());

        Map<String, String> expectedInfo = parse(segment.getSegmentInfo());
        assertEquals(expectedInfo, metaData.info());
    }

    private static Map<String, String> parse(String segmentInfo) {
        JsopTokenizer tokenizer = new JsopTokenizer(segmentInfo);
        tokenizer.read('{');
        return JsonObject.create(tokenizer).getProperties();
    }

    @Test
    public void hexDump() {
        assertEquals(segment.toString(), segmentWrapper.hexDump(true));
    }
}
