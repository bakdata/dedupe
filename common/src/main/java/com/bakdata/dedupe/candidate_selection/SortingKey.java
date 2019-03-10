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
package com.bakdata.dedupe.candidate_selection;

import java.util.function.Function;
import lombok.NonNull;
import lombok.Value;

/**
 * A sorting key allows a dataset to be indexed by a specific (calculated) value of a record, such that duplicates have
 * a higher probability of being in close proximity and thus a {@link CandidateSelection} may prune the search space
 * drastically.
 * <p>Note to increase the flexibility </p>
 *
 * @param <T> the type of the record.
 */
@Value
public class SortingKey<T, K extends Comparable<K>> {
    /**
     * The name of the sorting key. Mostly used for debugging.
     */
    @NonNull
    String name;
    /**
     * A calculation or simple value access to extract the key.
     * <p>Please try to avoid string concatenation and use {@link CompositeValue} unless you really want that
     * semantics. String concatenation will change the sorting order if the strings in the beginning do not have the
     * same length.</p>
     * <p>Example: Consider the two records [Ed, Sheeran] and [Edgar, Poe]. Concatentation will change order {@code
     * EdgarPoe < EdSheeran}.</p>
     *
     * @see CompositeValue
     */
    @NonNull
    Function<T, K> keyExtractor;
}
