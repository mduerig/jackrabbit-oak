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
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
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
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;

/**
 * michid document
 */
public final class SegmentGraph {
    private SegmentGraph() { }

    public interface SegmentGraphVisitor {
        void accept(@Nonnull UUID from, @CheckForNull UUID to);
    }

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
    public static void writeSegmentGraph(
            @Nonnull ReadOnlyStore fileStore,
            @Nonnull OutputStream out,
            @Nonnull Date epoch) throws Exception {

        checkNotNull(epoch);
        PrintWriter writer = new PrintWriter(checkNotNull(out));
        try {
            SegmentNodeState root = checkNotNull(fileStore).getHead();

            Graph<UUID> segmentGraph = parseSegmentGraph(fileStore);
            Graph<UUID> headGraph = parseHeadGraph(root.getRecordId());

            writer.write("nodedef>name VARCHAR, label VARCHAR, type VARCHAR, wid VARCHAR, gc INT, t INT, head BOOLEAN\n");
            for (UUID segment : segmentGraph.vertices) {
                writeNode(segment, writer, headGraph.vertices.contains(segment), epoch, fileStore.getTracker());
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, head BOOLEAN\n");
            for (Entry<UUID, Set<UUID>> edge : segmentGraph.edges.entrySet()) {
                UUID from = edge.getKey();
                for (UUID to : edge.getValue()) {
                    if (!from.equals(to)) {
                        Set<UUID> he = headGraph.edges.get(from);
                        boolean inHead = he != null && he.contains(to);
                        writer.write(from + "," + to + "," + inHead + "\n");
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    @Nonnull
    public static Graph<UUID> parseSegmentGraph(@Nonnull ReadOnlyStore fileStore) throws IOException {
        SegmentNodeState root = checkNotNull(fileStore).getHead();
        HashSet<UUID> roots = newHashSet(root.getRecordId().asUUID());
        return parseSegmentGraph(fileStore, roots, Functions.<UUID>identity());
    }

    public static void writeGCGraph(@Nonnull ReadOnlyStore fileStore, @Nonnull OutputStream out)
            throws Exception {
        PrintWriter writer = new PrintWriter(checkNotNull(out));
        try {
            SegmentNodeState root = checkNotNull(fileStore).getHead();
            Graph<Integer> gcGraph = parseGCGraph(fileStore);

            writer.write("nodedef>name VARCHAR\n");
            for (Integer gen : gcGraph.vertices) {
                writer.write(gen + "\n");
            }

            writer.write("edgedef>node1 VARCHAR, node2 VARCHAR, head BOOLEAN\n");
            for (Entry<Integer, Set<Integer>> edge : gcGraph.edges.entrySet()) {
                Integer from = edge.getKey();
                for (Integer to : edge.getValue()) {
                    if (!from.equals(to)) {
                        writer.write(from + "," + to + "\n");
                    }
                }
            }
        } finally {
            writer.close();
        }
    }

    @Nonnull
    public static Graph<Integer> parseGCGraph(@Nonnull final ReadOnlyStore fileStore)
            throws IOException {
        SegmentNodeState root = checkNotNull(fileStore).getHead();
        HashSet<UUID> roots = newHashSet(root.getRecordId().asUUID());
        return parseSegmentGraph(fileStore, roots, new Function<UUID, Integer>() {
            @Override @Nullable
            public Integer apply(UUID segmentId) {
                String info = getSegmentInfo2(segmentId, fileStore.getTracker());
                if (info != null) {
                    int i = info.indexOf("gc=");
                    if (i == -1) {
                        return -1;
                    }
                    int j = info.indexOf(',', i);
                    String gc = info.substring(i + 3, j);
                    return toInt(gc, -1);
                } else {
                    return -1;
                }
            }
        });
    }

    public static int toInt(String number, int defaultValue) {
        if (number == null) {
            return defaultValue;
        } else {
            try {
                return Integer.parseInt(number);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
    }

    @Nonnull
    public static <T> Graph<T> parseSegmentGraph(
            @Nonnull ReadOnlyStore fileStore,
            @Nonnull Set<UUID> roots,
            @Nonnull final Function<UUID, T> homomorphism) throws IOException {
        final Graph<T> graph = new Graph<T>();

        checkNotNull(homomorphism);
        checkNotNull(fileStore).traverseSegmentGraph(checkNotNull(roots),
            new SegmentGraphVisitor() {
                @Override
                public void accept(@Nonnull UUID from, @CheckForNull UUID to) {
                    graph.addVertex(homomorphism.apply(from));
                    if (to != null) {
                        graph.addVertex((homomorphism.apply(to)));
                        graph.addEdge(homomorphism.apply(from), homomorphism.apply(to));
                    }
                }
            });
        return graph;
    }

    @Nonnull
    public static Graph<UUID> parseHeadGraph(@Nonnull RecordId root) {
        final Graph<UUID> graph = new Graph<UUID>();

        new SegmentParser() {
            private void addEdge(RecordId from, RecordId to) {
                graph.addVertex(from.asUUID());
                graph.addVertex(to.asUUID());
                graph.addEdge(from.asUUID(), to.asUUID());
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
        }.parseNode(checkNotNull(root));
        return graph;
    }

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

    private static String getSegmentInfo2(UUID node, SegmentTracker tracker) {
        if (isDataSegmentId(node.getLeastSignificantBits())) {
            SegmentId id = tracker.getSegmentId(node.getMostSignificantBits(), node.getLeastSignificantBits());
            String info = id.getSegment().getSegmentInfo();
            return info;
        } else {
            return null;
        }
    }

    public static class Graph<T> {
        public final Set<T> vertices = newHashSet();
        public final Map<T, Set<T>> edges = newHashMap();

        private void addVertex(T vertex) {
            vertices.add(vertex);
        }

        private void addEdge(T from, T to) {
            Set<T> tos = edges.get(from);
            if (tos == null) {
                tos = newHashSet();
                edges.put(from, tos);
            }
            tos.add(to);
        }
    }
}
