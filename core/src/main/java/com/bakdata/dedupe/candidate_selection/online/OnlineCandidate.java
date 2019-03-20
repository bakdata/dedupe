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

import lombok.NonNull;
import lombok.Value;


/**
 * Represents a candidate pair that was generated with an {@link OnlineCandidateSelection}.
 *
 * @param <T> the type of the records.
 */
@Value
public class OnlineCandidate<T> implements com.bakdata.dedupe.candidate_selection.Candidate<T> {
    /**
     * The new record that triggered the {@link OnlineCandidateSelection}.
     */
    @NonNull
    T newRecord;
    /**
     * The old record already known to the {@link OnlineCandidateSelection}.
     */
    @NonNull
    T oldRecord;

    @Override
    public @NonNull T getRecord1() {
        return this.newRecord;
    }

    @Override
    public @NonNull T getRecord2() {
        return this.oldRecord;
    }
}
