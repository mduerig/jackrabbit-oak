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

package org.apache.jackrabbit.oak.segment.file.tar;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

import java.io.File;
import java.util.List;

import javax.annotation.Nonnull;

public class CompositeIOMonitor implements IOMonitor {
    private final List<IOMonitor> ioMonitors;

    public CompositeIOMonitor(@Nonnull IOMonitor... ioMonitors) {
        this.ioMonitors = newArrayList(checkNotNull(ioMonitors));
    }

    public CompositeIOMonitor(@Nonnull Iterable<IOMonitor> ioMonitors) {
        this.ioMonitors = newArrayList(checkNotNull(ioMonitors));
    }

    @Override
    public void beforeSegmentRead(File file, long msb, long lsb, int length) {
        ioMonitors.forEach(ioMonitor ->
                ioMonitor.beforeSegmentRead(file, msb, lsb, length));
    }

    @Override
    public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
        ioMonitors.forEach(ioMonitor ->
                ioMonitor.afterSegmentRead(file, msb, lsb, length, elapsed));
    }

    @Override
    public void beforeSegmentWrite(File file, long msb, long lsb, int length) {
        ioMonitors.forEach(ioMonitor ->
                ioMonitor.beforeSegmentWrite(file, msb, lsb, length));
    }

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        ioMonitors.forEach(ioMonitor ->
                ioMonitor.afterSegmentWrite(file, msb, lsb, length, elapsed));
    }
}
