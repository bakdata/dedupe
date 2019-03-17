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

package com.bakdata.dedupe.similarity;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.text.similarity.SimilarityScore;

/**
 * Used to translate {@link SimilarityScore} of apache common that are actually distance functions to similarity scores.
 */
@RequiredArgsConstructor
public class DistanceAdapter<T extends CharSequence> implements SimilarityMeasure<T> {
    private final SimilarityScore<? extends Number> score;

    @Override
    public double getSimilarity(final CharSequence left, final CharSequence right,
            @NonNull final SimilarityContext context) {
        return 0;
    }

    @Override
    public double getNonNullSimilarity(final @NonNull CharSequence left, final @NonNull CharSequence right,
            final @NonNull SimilarityContext context) {
        final double distance = this.score.apply(left, right).floatValue();
        if (distance == -1) {
            return 0;
        }
        return 1.0d - distance / Math.max(left.length(), right.length());
    }
}
