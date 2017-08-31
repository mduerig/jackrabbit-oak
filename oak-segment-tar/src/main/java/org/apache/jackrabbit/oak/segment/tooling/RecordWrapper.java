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
import static org.apache.jackrabbit.oak.segment.tooling.TypeMap.toToolType;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.Record;
import org.apache.jackrabbit.oak.tooling.filestore.RecordId;

public class RecordWrapper implements Record {
    @Nonnull
    private final Segment segment;
    @Nonnull
    private final RecordType type;
    private final int offset;

    public RecordWrapper(@Nonnull Segment segment, @Nonnull RecordType type, int offset) {
        this.segment = checkNotNull(segment);
        this.type = checkNotNull(type);
        this.offset = offset;
    }

    @Nonnull
    @Override
    public RecordId id() {
        return new RecordId(segment.getSegmentId().asUUID(), offset);
    }

    @Nonnull
    @Override
    public Type type() {
        return toToolType(type);
    }
}
