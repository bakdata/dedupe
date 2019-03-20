/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.bakdata.dedupe.duplicate_detection.online;

import com.bakdata.dedupe.clustering.Cluster;
import com.bakdata.dedupe.duplicate_detection.DuplicateDetection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.NonNull;

/**
 * An online duplicate detection algorithm processes a stream of records and returns all changed {@link Cluster}s for
 * each record.
 * <p>Consider a stream of records A, B, A' and A, A' being duplicates. The resulting stream will be [(A)], [(B)], [(A,
 * A')].</p>
 * <p>Now consider that we add A", which will result in a duplicate (A', A") and at the same time will cause (A, A')
 * not being considered a duplicate any longer. The result of A" will be [(A', A"), (A)].</p>
 * <p>The actual implementation may use any means necessary to find duplicates and to ensure proper transitivity ((A,B)
 * is duplicate and (B,C) is duplicate implies that (A,C) is duplicate).</p>
 *
 * @implSpec It is assumed that the cluster containing the new record will be the first element of the cluster list.
 */
@FunctionalInterface
public interface OnlineDuplicateDetection<C extends Comparable<C>, T> extends DuplicateDetection<C, T> {
    /**
     * Returns all clusters that have been affected by the new record.
     *
     * @param newRecord the new record.
     * @return all affected clusters.
     * @implSpec It is assumed that the cluster containing the new record will be the first element of the cluster
     * list.
     */
    @NonNull Iterable<Cluster<C, T>> detectDuplicates(@NonNull T newRecord);

    @Override
    default @NonNull Iterable<Cluster<C, T>> detectDuplicates(@NonNull Iterable<? extends T> records) {
        return StreamSupport.stream(records.spliterator(), false)
                .flatMap(record -> StreamSupport.stream(detectDuplicates(record).spliterator(), false))
                .collect(Collectors.toMap(Cluster::getId, Function.identity()))
                .values();
    }
}
