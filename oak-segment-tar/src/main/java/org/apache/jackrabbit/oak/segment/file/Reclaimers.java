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
 */

package org.apache.jackrabbit.oak.segment.file;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

class Reclaimers {

    private Reclaimers() {
        // Prevent instantiation.
    }

    static Predicate<GCGeneration> newOldReclaimer(
            @Nonnull final GCGeneration referenceGeneration,
            int retainedGenerations) {
        return new Predicate<GCGeneration>() {

            @Override
            public boolean apply(GCGeneration generation) {
                int retainedTails = retainedGenerations - (referenceGeneration.getGeneration() + 1);
                if (retainedTails > 0) {
                    if (referenceGeneration.compareFullGenerationWith(generation) <= retainedTails) {
                        return false;
                        // michid cleanup of no compacted segments from previous tail is missing.
                        // However with the proposed generation scheme there is no way to detect them.
                    }
                }

                if (referenceGeneration.getFullGeneration() == generation.getFullGeneration()) {
                    if (generation.isCompacted()) {
                        return false;
                    }

                    if (referenceGeneration.compareWith(generation) < retainedGenerations) {
                        return false;
                    }
                }

                return true;
            }

            @Override
            public String toString() {
                return String.format(
                        "(generation older than %d.%d, with %d retained generations)",
                        referenceGeneration.getGeneration(),
                        referenceGeneration.getFullGeneration(),
                        retainedGenerations
                );
            }

        };
    }

    static Predicate<GCGeneration> newExactReclaimer(@Nonnull final GCGeneration referenceGeneration) {
        return new Predicate<GCGeneration>() {
            @Override
            public boolean apply(GCGeneration generation) {
                return generation.equals(referenceGeneration);
            }
            @Override
            public String toString() {
                return "(generation==" + referenceGeneration + ")";
            }
        };
    }

}
