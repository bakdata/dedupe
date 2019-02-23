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

import lombok.Value;

@Value
public class CutoffSimiliarityMeasure<T> implements SimilarityMeasure<T> {
    SimilarityMeasure<T> inner;
    float threshold;

    protected static float cutoff(final float similarity, final float min) {
        return similarity < min ? 0 : similarity;
    }

    @Override
    public float getSimilarity(final T left, final T right, final SimilarityContext context) {
        return cutoff(this.inner.getSimilarity(left, right, context), this.threshold);
    }

    @Override
    public SimilarityMeasure<T> cutoff(final float threshold) {
        if (threshold < this.threshold) {
            return this;
        }
        return new com.bakdata.deduplication.similarity.CutoffSimiliarityMeasure<>(this.inner, threshold);
    }
}