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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import javax.annotation.Nonnull;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.rules.TemporaryFolder;

public class TemporaryFileStoreWithToolAPI extends TemporaryFileStore {
    private File directory;
    private FileStore fileStore;
    private FileStoreWrapper toolAPI;

    public TemporaryFileStoreWithToolAPI(TemporaryFolder folder) {
        super(folder, false);
    }

    @Nonnull
    public File getDirectory() {
        checkState(directory != null);
        return directory;
    }

    @Nonnull
    public FileStore getFileStore() {
        checkState(fileStore != null);
        return fileStore;
    }

    @Nonnull
    public FileStoreWrapper getToolAPI() {
        checkState(toolAPI != null);
        return toolAPI;
    }

    @Nonnull
    @Override
    protected FileStore build(@Nonnull FileStoreBuilder builder, @Nonnull File directory)
    throws IOException, InvalidFileStoreVersionException {
        this.directory = directory;
        IOMonitorBridge ioMonitor = new IOMonitorBridge();
        fileStore = builder
            .withMaxFileSize(1)
            .withIOMonitor(ioMonitor)
            .withProbes((fileStoreProbe, tarProbe) ->
                    toolAPI = new FileStoreWrapper(fileStoreProbe, tarProbe, ioMonitor::addIOMonitor))
            .build();

        SegmentNodeState head = fileStore.getHead();
        SegmentNodeState newHead = addContent(head.builder()).getNodeState();
        assertTrue(fileStore.getRevisions().setHead(
                head.getRecordId(), newHead.getRecordId()));
        fileStore.flush();
        return fileStore;
    }

    @Nonnull
    public static <B extends NodeBuilder> B addContent(@Nonnull B builder) {
        checkNotNull(builder);
        builder.setChildNode("a").setChildNode("aa").setProperty("x", 1);
        builder.setChildNode("b").setChildNode("bb").setProperty("y", 2);
        builder.setChildNode("c").setChildNode("cc").setProperty("z", 3);
        builder.setProperty("u", 1);
        builder.setProperty("v", 2);
        builder.setProperty("w", 3);
        return builder;
    }

    @Override
    protected void after() {
        try (Closer closer = Closer.create()) {
            closer.register(() -> super.after());
            closer.register(toolAPI);
        } catch (IOException e) {
            throw new IllegalStateException("Error closing the store", e);
        }
    }
}
