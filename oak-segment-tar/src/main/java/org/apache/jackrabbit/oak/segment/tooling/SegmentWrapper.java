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
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.asByte;
import static org.apache.jackrabbit.oak.tooling.filestore.Segment.Type.BULK;
import static org.apache.jackrabbit.oak.tooling.filestore.Segment.Type.DATA;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.tooling.filestore.Record;
import org.apache.jackrabbit.oak.tooling.filestore.SegmentMetaData;

public class SegmentWrapper implements org.apache.jackrabbit.oak.tooling.filestore.Segment {

    private final Segment segment;

    public SegmentWrapper(@Nonnull Segment segment) {
        this.segment = checkNotNull(segment);
    }

    @Nonnull
    @Override
    public UUID id() {
        return segment.getSegmentId().asUUID();
    }

    @Override
    public int size() {
        return segment.size();
    }

    @Nonnull
    @Override
    public Type type() {
        return segment.getSegmentId().isDataSegmentId()
            ? DATA
            : BULK;
    }

    @Nonnull
    @Override
    public Iterable<UUID> references() {
        List<UUID> uuids = newArrayList();
        for (int k = 0; k < segment.getReferencedSegmentIdCount(); k++) {
            uuids.add(segment.getReferencedSegmentId(k));
        }
        return uuids;
    }

    @Nonnull
    @Override
    public Iterable<Record> records() {
        List<Record> records = newArrayList();
        segment.forEachRecord((number, type, offset) ->
                records.add(new RecordWrapper(segment, type, offset)));
        return records;
    }

    @Override
    @Nonnull
    public SegmentMetaData metaData() {
        return new SegmentMetaData() {
            @Override
            public int version() {
                return asByte(segment.getSegmentVersion());
            }

            @Override
            public int generation() {
                return segment.getGcGeneration().getGeneration();
            }

            @Override
            public int fullGeneration() {
                return segment.getGcGeneration().getFullGeneration();
            }

            @Override
            public boolean compacted() {
                return segment.getGcGeneration().isCompacted();
            }

            @Override
            @Nonnull
            public Map<String, String> info() {
                String segmentInfo = segment.getSegmentInfo();
                return segmentInfo == null
                    ? emptyMap()
                    : parse(segmentInfo);
            }

            @Nonnull
            private Map<String, String> parse(@Nonnull String segmentInfo) {
                JsopTokenizer tokenizer = new JsopTokenizer(segmentInfo);
                tokenizer.read('{');
                return JsonObject.create(tokenizer).getProperties();
            }
        };
    }

    @Nonnull
    @Override
    public String hexDump(boolean includeHeader) {
        try (
            StringWriter string = new StringWriter();
            PrintWriter writer = new PrintWriter(string);
            WriterOutputStream out = new WriterOutputStream(writer, Charsets.UTF_8))
        {
            if (includeHeader) {
                segment.dumpHeader(out);
                writer.println("--------------------------------------------------------------------------");
            }
            segment.dumpHex(out);
            writer.println("--------------------------------------------------------------------------");
            return string.toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
