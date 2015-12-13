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

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static org.apache.commons.io.IOUtils.copy;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentGraph.writeSegmentGraph;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.jackrabbit.oak.plugins.segment.file.FileStore.ReadOnlyStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * michid document
 */
public class SegmentGraphTest {
    private final File targetDir = new File("target/segment-graph-test");
    private final File storeDir = new File(targetDir, "FileStore");
    private final File expectedGraph = new File(targetDir, "expected-graph.gdf");;

    private ReadOnlyStore fileStore;

    @Before
    public void setup() throws IOException {
        unzip(SegmentGraphTest.class.getResourceAsStream("file-store.zip"), targetDir);
        fileStore = new ReadOnlyStore(storeDir);
        copyToFile(SegmentGraphTest.class.getResourceAsStream(expectedGraph.getName()), expectedGraph);
    }

    @After
    public void tearDown() {
        fileStore.close();
        targetDir.delete();
    }

    @Test
    public void compareGraphs() throws Exception {
        File actualGraph = new File(targetDir, "actual.gdf");
        Calendar epoch = Calendar.getInstance();
        epoch.set(YEAR, 2015);
        epoch.set(MONTH, 11);
        epoch.set(DAY_OF_MONTH, 13);
        epoch.set(HOUR_OF_DAY, 0);
        epoch.set(MINUTE, 0);
        epoch.set(SECOND, 0);
        epoch.set(MILLISECOND, 0);
        writeSegmentGraph(fileStore, new FileOutputStream(actualGraph), epoch.getTime());
        assertEqualContent(expectedGraph, actualGraph);
    }

    private static void assertEqualContent(File expectedGraph, File actualGraph) throws IOException {
        assertEquals(expectedGraph + "!=" + actualGraph, toString(expectedGraph), toString(actualGraph));
    }

    private static String toString(File textFile) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(textFile)));
        try {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } finally {
            reader.close();
        }
    }

    private static void unzip(InputStream is, File targetDir) throws IOException {
        final ZipInputStream zis = new ZipInputStream(is);
        try {
            ZipEntry entry;
            while((entry = zis.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    new File(targetDir, entry.getName()).mkdirs();
                } else {
                    File target = new File(targetDir, entry.getName());
                    target.getParentFile().mkdirs();
                    copyToFile(zis, target);
                }
            }
        } finally {
            zis.close();
        }
    }

    private static void copyToFile(InputStream is, File target) throws IOException {
        OutputStream out = new FileOutputStream(target);
        try {
            copy(is, out);
        } finally {
            out.close();
        }
    }

}
