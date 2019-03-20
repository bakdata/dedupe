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

package com.bakdata.dedupe.deduplication;

import com.bakdata.util.StreamUtil;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;


/**
 * A full deduplication process, which ensures that no duplicate record is emitted.
 * <p>In general, all implementations will ensure that the user receives a duplicate-free datasets, either through
 * repeated updates or by providing a duplicate-free dataset.</p>
 * <p>The actual implementation may use any means necessary to find duplicates, to ensure proper transitivity ((A,B) is
 * duplicate and (B,C) is duplicate implies that (A,C) is duplicate), and to give a resulting representation.</p>
 *
 * @param <T> the type of the record
 */
@FunctionalInterface
public interface Deduplication<T> {
    /**
     * Deduplicates the dataset.
     * <p>Note that for online algorithms, duplicates will be repeatedly emitted with updated
     * representation, since it is impossible to suppress earlier emission without blocking execution. The user needs to
     * invalidate earlier results through external means (for example, putting them in a key-value store with the key
     * being the id of the duplicate).</p>
     *
     * @param records the record that should be freed from duplicates.
     * @return a duplicate-free dataset with the above mentioned limitation for online algorithms.
     */
    @NonNull Stream<T> deduplicate(@NonNull Stream<? extends T> records);

    /**
     * Deduplicates the dataset.
     * <p>For online algorithms this method can only be applied on a finite stream and could be used to verify results
     * in a test or compare performance to an offline algorithm.</p>
     * <p>For online algorithms, this method may emit duplicates repeatedly with the same id, since an online algorithm
     * usually eagerly emits results. To ensure a real duplicate-free dataset, use {@link
     * #materializedDeduplicate(Iterable, Function)}.</p>
     *
     * @param records the record that should be freed from duplicates.
     * @return a duplicate-free dataset with the above mentioned limitation for online algorithms.
     * @implSpec For online algorithms, it is strongly encouraged that duplicates are filtered such that only the final
     * representation remains.
     */
    default @NonNull Collection<T> materializedDeduplicate(final @NonNull Iterable<? extends T> records) {
        return this.deduplicate(StreamUtil.stream(records)).collect(Collectors.toList());
    }

    /**
     * Selects the candidates for the given records and materializes them. The additional idExtractor ensures that
     * previously found duplicates are removed from the output.
     * <p>For online algorithms this method can only be applied on a finite stream and could be used to verify results
     * in a test or compare performance to an offline algorithm.</p>
     *
     * @param records the record that should be freed from duplicates.
     * @return a duplicate-free dataset.
     * @implSpec For online algorithms, it is strongly encouraged that duplicates are filtered such that only the final
     * representation remains.
     */
    default @NonNull Collection<T> materializedDeduplicate(final @NonNull Iterable<? extends T> records,
            final @NonNull Function<? super T, Object> idExtractor) {
        return this.deduplicate(StreamUtil.stream(records))
                .collect(Collectors.toMap(idExtractor, Function.identity()))
                .values();
    }
}
