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

import com.bakdata.util.StreamUtil;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;


/**
 * Selects candidates from a static or dynamic dataset accessible through Iterables.
 * <p>The dataset might be stored off-heap and retrieved on demand.</p>
 *
 * @param <T> the type of the records.
 */
@FunctionalInterface
public interface CandidateSelection<T> {
    /**
     * Selects the candidates for the given records.
     *
     * @param records the dataset for which the candidates are generated.
     * @return the generated candidates.
     * @implSpec It is assumed that this method will work stateless. Derivations need to be documented.
     * @implSpec The output of the method should be repeatable traversable. Derivations need to be documented.
     */
    @NonNull Stream<Candidate<T>> selectCandidates(@NonNull Stream<? extends T> records);

    /**
     * Selects the candidates for the given records and materializes them.
     * <p>For online algorithms this method can only be applied on a finite stream.</p>
     *
     * @param records the dataset for which the candidates are generated.
     * @return the generated candidates.
     * @implSpec It is assumed that this method will work stateless. Derivations need to be documented.
     */
    default @NonNull Collection<Candidate<T>> selectCandidates(final @NonNull Iterable<? extends T> records) {
        return this.selectCandidates(StreamUtil.stream(records)).collect(Collectors.toList());
    }
}
