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
package com.bakdata.dedupe.candidate_selection.online;

import com.bakdata.dedupe.candidate_selection.Candidate;
import java.util.stream.Stream;
import lombok.NonNull;

/**
 * Selects candidates from a streaming dataset by processing the incoming elements one-at-a-time.
 *
 * @param <T> the type of the records.
 * @implSpec Implementations should outline the complexity.
 */
@FunctionalInterface
public interface OnlineCandidateSelection<T>
        extends com.bakdata.dedupe.candidate_selection.CandidateSelection<T> {
    /**
     * Selects the candidates for the a new incoming record. The selection algorithm will maintain an internal
     * representation of the previously seen records or a subset thereof.
     * <p>Thus, this method is stateful.</p>
     *
     * @param newRecord the new record for which the candidates are generated.
     * @return the generated candidates.
     */
    @NonNull Stream<Candidate<T>> selectCandidates(@NonNull T newRecord);

    /**
     * @implNote Repeatedly invokes {@link #selectCandidates(Object)} to get all candidates.
     */
    @Override
    default @NonNull Stream<Candidate<T>> selectCandidates(@NonNull Stream<? extends T> records) {
        return records.flatMap(record -> selectCandidates(record));
    }
}
