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
package com.bakdata.dedupe.deduplication.online;

import java.util.stream.Stream;
import lombok.NonNull;

/**
 * A full online deduplication process, which ensures that no duplicate record is emitted.
 * <p>Consider a stream of records A, B, A' and A, A' being duplicates. The resulting stream will be A, B, A", where A"
 * is a suitable representation of A and A'.</p>
 * <p>The actual implementation may use any means necessary to find duplicates, to ensure proper transitivity ((A,B) is
 * duplicate and (B,C) is duplicate implies that (A,C) is duplicate), and to give a resulting representation.</p>
 *
 * @implSpec It is assumed that any duplicate A' of a previous value A results in a returned representation with the
 * same identifier as A. Implementations should document under which conditions this assumptions holds.
 */
@FunctionalInterface
public interface OnlineDeduplication<T> extends com.bakdata.dedupe.deduplication.Deduplication<T> {
    /**
     * Deduplicates the record with all previously seen records.
     * <p>There are two cases:</p>
     * <ul>
     * <li>No duplicate has been detected: The record itself is returned.</li>
     * <li>At least one duplicate has been detected: The record is added to the duplicate {@link
     * com.bakdata.dedupe.clustering.Cluster} and a suitable representation for that cluster is returned.</li>
     * </ul>
     * @param newRecord the record that should be processed with all previously seen records
     * @return the record or a representation of the duplicate {@link com.bakdata.dedupe.clustering.Cluster} of
     * the record.
     */
    @NonNull T deduplicate(@NonNull T newRecord);

    @Override
    default @NonNull Stream<T> deduplicate(@NonNull Stream<? extends T> records) {
        return records.map(this::deduplicate);
    }
}
