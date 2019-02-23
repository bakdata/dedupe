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

package com.bakdata.deduplication.similarity;

import org.apache.commons.text.similarity.LevenshteinDistance;

public class Levensthein<T extends CharSequence> implements SimilarityMeasure<T> {
    private final float threshold;

    public Levensthein(final float threshold) {
        this.threshold = threshold;
    }

    @Override
    public float getSimilarity(final CharSequence left, final CharSequence right, final SimilarityContext context) {
        final var maxLen = CommonSimilarityMeasures.getMaxLen(left, right);
        final var maxDiff = (int) (maxLen * (1 - this.threshold));
        final var measure = new DistanceSimilarityMeasure<T>(new LevenshteinDistance(maxDiff));
        return measure.getSimilarity(left, right, context);
    }

    @Override
    public SimilarityMeasure<T> cutoff(final float threshold) {
        if (threshold < this.threshold) {
            return this;
        }
        return new Levensthein<>(threshold);
    }
}
