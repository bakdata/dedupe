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

import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.toSimilarity;

import lombok.NonNull;
import lombok.Value;
import org.apache.commons.text.similarity.LevenshteinDistance;


/**
 * Provides the Levenshtein similarity calculation, which calculates the number of insertions, deletions, and
 * replacements needed to transform one string into another.
 * <p>The similarity is calculated by normalizing the number of edits over the maximum input length.</p>
 * <p>Note that Levenshtein distance is really slow, but can be tremulously sped up by using {@link #cutoff(double)}
 * or {@link #scaleWithThreshold(double)}.</p>
 *
 * @param <T> the type of the input.
 */
@Value
public class Levenshtein<T extends CharSequence> implements SimilarityMeasure<T> {
    private static final SimilarityMeasure<CharSequence> NoThresholdMeasure =
            toSimilarity(new LevenshteinDistance());
    /**
     * The threshold [0; 1], below which the calculation should be aborted. A high threshold saves tremendous time.
     */
    double threshold;

    @Override
    public double getNonNullSimilarity(final @NonNull CharSequence left, final @NonNull CharSequence right,
            final SimilarityContext context) {
        if (this.threshold <= 0) {
            return NoThresholdMeasure.getNonNullSimilarity(left, right, context);
        }
        final int maxLen = Math.max(left.length(), right.length());
        final int maxDiff = (int) (maxLen * (1 - this.threshold));
        final SimilarityMeasure<CharSequence> measure = toSimilarity(new LevenshteinDistance(maxDiff));
        return measure.getNonNullSimilarity(left, right, context);
    }

    @Override
    public boolean isSymmetric() {
        return true;
    }

    @Override
    public @NonNull SimilarityMeasure<T> cutoff(final double threshold) {
        if (threshold < this.threshold) {
            return this;
        }
        return new Levenshtein<>(threshold);
    }
}
