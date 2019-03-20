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

package com.bakdata.dedupe.classifier;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Contains the possible classification classes.
 */
@RequiredArgsConstructor
public enum Classification {
    /**
     * A sure duplicate.
     */
    DUPLICATE(false),
    /**
     * Possible duplicate may come from a high uncertainty of the {@link Classifier}. For example, if the classifier
     * uses a {@link com.bakdata.dedupe.similarity.SimilarityMeasure} and a threshold, the classifier might have two
     * thresholds:
     * <ul>
     * <li>an upper threshold which defines when a pair is a {@link #DUPLICATE}, and</li>
     * <li>a lower bound when defines all pairs as {@link #NON_DUPLICATE}.</li>
     * </ul>
     * All similarities between the threshold would then be labeled POSSIBLE_DUPLICATE.
     */
    POSSIBLE_DUPLICATE(true),
    /**
     * A sure non-duplicate.
     */
    NON_DUPLICATE(false),
    /**
     * Unknown classifications are primarily caused by a lack of information. For example, when two records have no two
     * fields jointly set (alternating null values), it is impossible to state if this pair is a duplicate or not.
     * <p>The different class from {@link #POSSIBLE_DUPLICATE} is especially useful in later clustering: An unknown
     * classification has no weight and this neither imply that a cluster is coherent or should be split. A possible
     * duplicate is, however, already a meaningful statement. If high precision is the overall goal, possible duplicates
     * might be cut off.</p>
     * <p>Another difference between the classes becomes apparent when using manual labeling for difficult pairs. While
     * possible duplicates are exactly those pairs that should be labeled, unknown pairs have a high probability to not
     * be resolvable by humans.</p>
     */
    UNKNOWN(true);

    /**
     * Returns if this class can directly be used
     */
    @Getter
    final boolean ambiguous;

    /**
     * @return true iff {@code this === UNKNOWN}
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
