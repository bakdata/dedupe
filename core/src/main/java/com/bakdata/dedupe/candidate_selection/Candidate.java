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


import com.bakdata.dedupe.candidate_selection.offline.OfflineCandidateSelection;
import lombok.NonNull;

/**
 * Represents a candidate pair that was generated with an {@link OfflineCandidateSelection}.
 * <p>The order of the records depends on the implementation of the candidate selection and should be viewed as random
 * unless explicitly stated by the selection.</p>
 *
 * @param <T> the type of the records.
 */
public interface Candidate<T> {
    /**
     * Returns the first record. Please check the candidate selection for specific semantics.
     *
     * @return the first record.
     */
    @NonNull T getRecord1();

    /**
     * Returns the second record. Please check the candidate selection for specific semantics.
     *
     * @return the second record.
     */
    @NonNull T getRecord2();
}
