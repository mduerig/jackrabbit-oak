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
import static java.nio.file.Files.readAttributes;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.tar.TarFiles.TarProbe;
import org.apache.jackrabbit.oak.tooling.filestore.Tar;

public class TarWrapper implements Tar {

    @Nonnull
    private final TarProbe.Tar tar;

    public TarWrapper(@Nonnull TarProbe.Tar tar) {
        this.tar = checkNotNull(tar);
    }

    @Nonnull
    @Override
    public String name() {
        return tar.file().getName();
    }

    @Override
    public long size() {
        return tar.file().length();
    }

    @Override
    public long timestamp() {
        try {
            Path path = Paths.get(tar.file().toURI());
            return readAttributes(path, BasicFileAttributes.class)
                .creationTime()
                .to(MILLISECONDS);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Nonnull
    @Override
    public Iterable<UUID> segmentIds() {
        return tar.segmentIds();
    }
}
