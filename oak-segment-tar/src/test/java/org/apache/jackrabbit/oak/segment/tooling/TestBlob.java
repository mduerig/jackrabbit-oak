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

import static java.lang.Long.signum;

import java.io.InputStream;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;

class TestBlob implements Blob {
    private final long blobSize;

    public TestBlob(long blobSize) {
        this.blobSize = blobSize;
    }

    @Nonnull
    @Override
    public InputStream getNewStream() {
        return new InputStream() {
            long pos = 0;

            @Override
            public int read() {
                return signum(blobSize - ++pos);
            }
        };
    }

    @Override
    public long length() {
        return blobSize;
    }

    @CheckForNull
    @Override
    public String getReference() {
        return null;
    }

    @CheckForNull
    @Override
    public String getContentIdentity() {
        return null;
    }
}
