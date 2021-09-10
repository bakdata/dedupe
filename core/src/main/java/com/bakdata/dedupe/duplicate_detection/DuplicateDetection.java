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

package com.bakdata.dedupe.duplicate_detection;

import com.bakdata.dedupe.clustering.Cluster;
import com.bakdata.util.StreamUtil;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;


/**
 * A duplicate detection algorithm processes a dataset of records and returns the distinct {@link Cluster}s.
 * <p>In general, all implementations will ensure that the user receives a complete clustering of the dataset, either
 * through repeated updates or by providing a duplicate-free dataset.</p>
 * <p>Consider a dataset of records A, B, A' and A, A' being duplicates. The final result will be [(A, A'), (B)]. For
 * online algorithms, there may be intermediate results, such as [(A)] and [(A), (B)], which the user needs to filter
 * using the cluster id.</p>
 * <p>Now consider that we add A", which will result in a duplicate (A', A") and at the same time will cause (A, A')
 * not being considered a duplicate any longer. The result of A" will be [(A', A"), (A), (B)].</p>
 * <p>The actual implementation may use any means necessary to find duplicates and to ensure proper transitivity ((A,B)
 * is duplicate and (B,C) is duplicate implies that (A,C) is duplicate).</p>
 *
 * @param <C> the type of the cluster id
 * @param <T> the type of the record
 * @implSpec It is assumed that the cluster containing the new record will be the first element of the cluster list.
 */
@FunctionalInterface
public interface DuplicateDetection<C extends Comparable<C>, T> {
    /**
     * Finds all duplicates in the dataset.
     * <p>Note that for online algorithms, duplicates will be repeatedly emitted with updated
     * representation, since it is impossible to suppress earlier emission without blocking execution. The user needs to
     * invalidate earlier results through external means (for example, putting them in a key-value store with the key
     * being the id of the duplicate).</p>
     *
     * @param records the records of which the duplicates should be detected.
     * @return the duplicates with the above mentioned limitation for online algorithms.
     */
    @NonNull Stream<Cluster<C, T>> detectDuplicates(@NonNull Stream<? extends T> records);

    /**
     * Finds all duplicates in the dataset.
     * <p>For online algorithms this method can only be applied on a finite stream and could be used to verify results
     * in a test or compare performance to an offline algorithm.</p>
     *
     * @param records the records of which the duplicates should be detected.
     * @return all duplicates of the dataset.
     */
    default @NonNull Collection<Cluster<C, T>> materializeDuplicates(final @NonNull Iterable<? extends T> records) {
        return this.detectDuplicates(StreamUtil.stream(records))
                .collect(Collectors.toMap(Cluster::getId, Function.identity()))
                .values();
    }
}
