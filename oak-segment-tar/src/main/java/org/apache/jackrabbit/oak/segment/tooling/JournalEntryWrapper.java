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
import static org.apache.jackrabbit.oak.segment.RecordId.offsetFormString;
import static org.apache.jackrabbit.oak.segment.RecordId.uuidFromString;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.JournalEntry;
import org.apache.jackrabbit.oak.tooling.filestore.RecordId;

public class JournalEntryWrapper implements org.apache.jackrabbit.oak.tooling.filestore.JournalEntry {
    @Nonnull
    private final JournalEntry entry;

    public JournalEntryWrapper(@Nonnull JournalEntry entry) {
        this.entry = checkNotNull(entry);
    }

    @Nonnull
    @Override
    public RecordId id() {
        return new RecordId(uuidFromString(entry.getRevision()), offsetFormString(entry.getRevision()));
    }

    @Override
    public long timestamp() {
        return entry.getTimestamp();
    }
}
