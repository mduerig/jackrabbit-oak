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
import static java.util.Objects.hash;

import java.io.InputStream;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.tooling.filestore.Binary;

public class BlobWrapper implements Binary {
    @Nonnull
    private final Blob blob;

    public BlobWrapper(@Nonnull Blob blob) {
        this.blob = checkNotNull(blob);
    }

    @Nonnull
    @Override
    public InputStream bytes() {
        return blob.getNewStream();
    }

    @Override
    public long size() {
        return blob.length();
    }

    @Override
    public boolean equals(Object other) {
        if (getClass() != other.getClass()) {
            throw new IllegalArgumentException(other.getClass() + " is not comparable with " + getClass());
        }
        if (this == other) {
            return true;
        }
        return blob.equals(((BlobWrapper) other).blob);
    }

    @Override
    public int hashCode() {
        return hash(blob);
    }

    @Override
    public String toString() {
        return "BlobWrapper{blob=" + blob + '}';
    }
}
