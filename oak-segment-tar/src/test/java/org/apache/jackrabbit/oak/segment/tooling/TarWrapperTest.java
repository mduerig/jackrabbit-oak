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

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import com.google.common.io.PatternFilenameFilter;
import org.apache.jackrabbit.oak.segment.SegmentNodeBuilder;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.tooling.filestore.Store;
import org.apache.jackrabbit.oak.tooling.filestore.Tar;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.kamranzafar.jtar.TarEntry;
import org.kamranzafar.jtar.TarInputStream;

public class TarWrapperTest {
    private static final Pattern TAR_ENTRY_PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
                    + "(\\.([0-9a-f]{8}))?(\\..*)?");


    @Nonnull
    private final TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Nonnull
    private final TemporaryFileStoreWithToolAPI tempFileStore = new TemporaryFileStoreWithToolAPI(tempFolder);

    @Nonnull @Rule
    public RuleChain chain = RuleChain
            .outerRule(tempFolder)
            .around(tempFileStore);

    private final List<UUID> expectedIds = newArrayList();

    private Store toolAPI;

    @Before
    public void setup() throws IOException {
        toolAPI = tempFileStore.getToolAPI();

        FileStore fileStore = tempFileStore.getFileStore();
        SegmentNodeState head = fileStore.getHead();
        SegmentNodeBuilder builder = head.builder();
        builder.setProperty("blob", new TestBlob(4096 * 4096));
        fileStore.getRevisions().setHead(head.getRecordId(), builder.getNodeState().getRecordId());
        fileStore.flush();

        File[] files = tempFileStore.getDirectory().listFiles(new PatternFilenameFilter(".*\\.tar"));
        assertNotNull(files);
        List<File> tars = reverse(asList(files));
        for (File tar : tars) {
            expectedIds.addAll(getIds(tar));
        }
    }

    @Nonnull
    private static List<UUID> getIds(@Nonnull File tarFile) throws IOException {
        try (TarInputStream tarStream = new TarInputStream(new FileInputStream(tarFile.getAbsolutePath()))) {
            List<UUID> ids = newArrayList();
            TarEntry tarEntry = tarStream.getNextEntry();
            while (tarEntry != null) {
                Matcher matcher = TAR_ENTRY_PATTERN.matcher(tarEntry.getName());
                if (matcher.matches()) {
                    UUID id = UUID.fromString(matcher.group(1));
                    ids.add(id);
                }
                tarEntry = tarStream.getNextEntry();
            }
            return reverse(ids);
        }
    }

    @Test
    public void testSegmentIds() {
        Iterable<UUID> actualIds = concat(transform(toolAPI.tars(), Tar::segmentIds));
        assertTrue(Iterables.elementsEqual(expectedIds, actualIds));
    }
}
