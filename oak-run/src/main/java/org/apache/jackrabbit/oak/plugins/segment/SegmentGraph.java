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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.SegmentGraphVisitor;

/**
 * michid document
 * michid @Nullable
 */
public final class SegmentGraph {
    private SegmentGraph() { }
    // michid clean up test resources when done

    /**
     * Write the segment graph of a file store to a stream.
     * <p>
     * The graph is written in
     * <a href="https://gephi.github.io/users/supported-graph-formats/gdf-format/">the Guess GDF format</a>,
     * which is easily imported into <a href="https://gephi.github.io/">Gephi</a>.
     * As GDF only supports integers but the segment time stamps are encoded as long
     * the {@code epoch} argument is used as a negative offset translating all timestamps
     * into a valid int range.
     *
     * @param fileStore     file store to graph
     * @param out           stream to write the graph to
     * @param epoch         epoch (in milliseconds)
     * @throws Exception
     */
    public static void writeSegmentGraph(ReadOnlyStore fileStore, OutputStream out, Date epoch) throws Exception {
        PrintWriter writer = new PrintWriter(out);
        try {
            SegmentNodeState root = fileStore.getHead();

            Graph segmentGraph = parseSegmentGraph(fileStore, root);
            Graph headGraph = parseHeadGraph(root.getRecordId());

            writer.write("nodedef>name VARCHAR, label VARCHAR, type VARCHAR, wid VARCHAR, gc INT, t INT, head BOOLEAN\n");
            for (UUID segment : segmentGraph.vertices) {
                writeNode(segment, writer, headGraph.vertices.contains(segment), epoch, fileStore.getTracker());
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, head BOOLEAN\n");
            for (Entry<UUID, Set<UUID>> edge : segmentGraph.edges.entrySet()) {
                UUID from = edge.getKey();
                for (UUID to : edge.getValue()) {
                    Set<UUID> he = headGraph.edges.get(from);
                    boolean inHead = he != null && he.contains(to);
                    writer.write(from + "," + to + "," + inHead + "\n");
                }
            }
        } finally {
            writer.close();
        }
    }

    public static Graph parseSegmentGraph(ReadOnlyStore fileStore, SegmentNodeState root) throws IOException {
        final Set<UUID> vertices = newHashSet();
        final Map<UUID, Set<UUID>> edges = newHashMap();

        fileStore.traverseSegmentGraph(new HashSet<UUID>(singleton(root.getRecordId().asUUID())),
            new SegmentGraphVisitor() {
                @Override
                public void accept(@Nonnull UUID from, @CheckForNull UUID to) {
                    if (to != null) {
                        vertices.add(from);
                        vertices.add(to);
                        Set<UUID> tos = edges.get(from);
                        if (tos == null) {
                            tos = newHashSet();
                            edges.put(from, tos);
                        }
                        tos.add(to);
                    }
                }
            });
        return new Graph(vertices, edges);
    }

    public static Graph parseHeadGraph(RecordId root) {
        final Set<UUID> vertices = newHashSet();
        final Map<UUID, Set<UUID>> edges = newHashMap();

        new SegmentParser() {
            private void addEdge(RecordId from, RecordId to) {
                UUID fromUUID = from.asUUID();
                UUID toUUID = to.asUUID();
                if (!fromUUID.equals(toUUID)) {
                    Set<UUID> tos = edges.get(fromUUID);
                    if (tos == null) {
                        tos = newHashSet();
                        edges.put(fromUUID, tos);
                    }
                    tos.add(toUUID);
                    vertices.add(fromUUID);
                    vertices.add(toUUID);
                }
            }
            @Override
            protected void onNode(RecordId parentId, RecordId nodeId) {
                super.onNode(parentId, nodeId);
                addEdge(parentId, nodeId);
            }
            @Override
            protected void onTemplate(RecordId parentId, RecordId templateId) {
                super.onTemplate(parentId, templateId);
                addEdge(parentId, templateId);
            }
            @Override
            protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMap(parentId, mapId, map);
                addEdge(parentId, mapId);
            }
            @Override
            protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapDiff(parentId, mapId, map);
                addEdge(parentId, mapId);
            }
            @Override
            protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapLeaf(parentId, mapId, map);
                addEdge(parentId, mapId);
            }
            @Override
            protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapBranch(parentId, mapId, map);
                addEdge(parentId, mapId);
            }
            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                super.onProperty(parentId, propertyId, template);
                addEdge(parentId, propertyId);
            }
            @Override
            protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
                super.onValue(parentId, valueId, type);
                addEdge(parentId, valueId);
            }
            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                super.onBlob(parentId, blobId);
                addEdge(parentId, blobId);
            }
            @Override
            protected void onString(RecordId parentId, RecordId stringId) {
                super.onString(parentId, stringId);
                addEdge(parentId, stringId);
            }
            @Override
            protected void onList(RecordId parentId, RecordId listId, int count) {
                super.onList(parentId, listId, count);
                addEdge(parentId, listId);
            }
            @Override
            protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
                super.onListBucket(parentId, listId, index, count, capacity);
                addEdge(parentId, listId);
            }
        }.parseNode(root);
        return new Graph(vertices, edges);
    }

    // michid implement parseGCGraph

    private static void writeNode(UUID node, PrintWriter writer, boolean inHead, Date epoch, SegmentTracker tracker) {
        Map<String, String> sInfo = getSegmentInfo(node, tracker);
        if (sInfo == null) {
            writer.write(node + ",b,bulk,b,-1,-1," + inHead + "\n");
        } else {
            long t = asLong(sInfo.get("t"));
            long ts = t - epoch.getTime();
            checkArgument(ts >= Integer.MIN_VALUE && ts <= Integer.MAX_VALUE,
                    "Time stamp (" + new Date(t) + ") not in epoch (" +
                    new Date(epoch.getTime() + Integer.MIN_VALUE) + " - " +
                    new Date(epoch.getTime() + Integer.MAX_VALUE) + ")");
            writer.write(node +
                    "," + sInfo.get("sno") +
                    ",data" +
                    "," + sInfo.get("wid") +
                    "," + sInfo.get("gc") +
                    "," + ts +
                    "," + inHead + "\n");
        }
    }

    private static long asLong(String string) {
        return Long.valueOf(string);
    }

    private static Map<String, String> getSegmentInfo(UUID node, SegmentTracker tracker) {
        if (isDataSegmentId(node.getLeastSignificantBits())) {
            SegmentId id = tracker.getSegmentId(node.getMostSignificantBits(), node.getLeastSignificantBits());
            String info = id.getSegment().getSegmentInfo();
            if (info != null) {
                JsopTokenizer tokenizer = new JsopTokenizer(info);
                tokenizer.read('{');
                return JsonObject.create(tokenizer).getProperties();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    public static class Graph {
        public final Set<UUID> vertices;
        public final Map<UUID, Set<UUID>> edges;

        private Graph(Set<UUID> vertices, Map<UUID, Set<UUID>> edges) {
            this.vertices = vertices;
            this.edges = edges;
        }
    }
}
