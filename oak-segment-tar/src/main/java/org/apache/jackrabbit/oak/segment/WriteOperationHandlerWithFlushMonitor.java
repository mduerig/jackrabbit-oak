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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriteOperationHandlerWithFlushMonitor implements WriteOperationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(WriteOperationHandlerWithFlushMonitor.class);

    @NotNull
    private final WriteOperationHandler delegate;

    @NotNull
    private final MeterStats flushMeter;

    private final long warnThreshold;
    private final long errorThreshold;

    @NotNull
    private final AtomicLong lastFlush = new AtomicLong(currentTimeMillis());

    @NotNull
    private final AtomicLong lastWarn = new AtomicLong(currentTimeMillis());

    @NotNull
    private final AtomicBoolean blocking = new AtomicBoolean(false);


    WriteOperationHandlerWithFlushMonitor(
            @NotNull WriteOperationHandler writeOperationHandler,
            int warnThreshold,
            int errorThreshold,
            @NotNull StatisticsProvider statsProvider) {
        checkArgument(warnThreshold > 0);
        checkArgument(errorThreshold == 0 || errorThreshold > warnThreshold);
        delegate = writeOperationHandler;
        this.warnThreshold = MILLISECONDS.convert(warnThreshold, SECONDS);
        this.errorThreshold = MILLISECONDS.convert(errorThreshold, SECONDS);
        flushMeter = statsProvider.getMeter("oak.segment.flush", METRICS_ONLY);
    }

    @Override
    @NotNull
    public RecordId execute(@NotNull WriteOperation writeOperation)
    throws IOException {
        long ts = currentTimeMillis();
        long dt = ts - lastFlush.get();
        if (errorThreshold > 0 && dt > errorThreshold) {
            LOG.error(
                "Transient write operations were not flushed for {} seconds. " +
                "Disallowing write operations.",
                SECONDS.convert(dt, MILLISECONDS));
            blocking.set(true);
            throw new IOException(
                "Write operations disallowed: transient write operations not flushed for too long.");
        } else if (dt > warnThreshold) {
            if (ts - lastWarn.get() > MILLISECONDS.convert(1, SECONDS)) {
                if (errorThreshold > 0) {
                    LOG.warn(
                        "Transient write operations were not flushed for {} seconds. " +
                        "{} seconds remaining until write operations will be disallowed.",
                        SECONDS.convert(dt, MILLISECONDS),
                        SECONDS.convert(errorThreshold - dt, MILLISECONDS));
                } else {
                    LOG.warn(
                        "Transient write operations were not flushed for {} seconds." +
                        SECONDS.convert(dt, MILLISECONDS));
                }
                lastWarn.set(ts);
            }
        }

        return delegate.execute(writeOperation);
    }

    @Override
    public void flush(@NotNull SegmentStore store) throws IOException {
        delegate.flush(store);
        flushMeter.mark();
        lastFlush.set(currentTimeMillis());
        if (blocking.getAndSet(false)) {
            LOG.info("Successful flush of transient write operations. Allowing write operations again.");
        }
    }
}
