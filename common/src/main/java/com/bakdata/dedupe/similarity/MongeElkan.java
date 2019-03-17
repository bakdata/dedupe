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

import java.util.Collection;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

@Value
class MongeElkan<C extends Collection<? extends E>, E> implements CollectionSimilarityMeasure<C, E> {
    private final SimilarityMeasure<E> pairMeasure;
    private final int maxPositionDiff;
    private final float cutoff;

    @Override
    public float calculateNonEmptyCollectionSimilarity(@NonNull final C leftCollection,
            @NonNull final C rightCollection, @NonNull final SimilarityContext context) {
        final List<E> leftList = List.copyOf(leftCollection);
        final List<E> rightList = List.copyOf(rightCollection);
        // consider a cutoff of .9 and |left| = 3, then on average each element has .1 buffer
        // as soon as the current sum + buffer < index, the cutoff threshold cannot be passed (buffer used up)
        final float cutoffBuffer = (1 - this.cutoff) * leftCollection.size();
        float sum = 0;
        for (int leftIndex = 0; leftIndex < leftCollection.size() && (cutoffBuffer + sum) >= leftIndex;
                leftIndex++) {
            float max = 0;
            for (int rightIndex = Math.max(0, leftIndex - this.maxPositionDiff),
                    rightMax = Math.min(rightCollection.size(), leftIndex + this.maxPositionDiff);
                    max < 1.0 && rightIndex < rightMax; rightIndex++) {
                max = Math.max(max, this.pairMeasure
                        .getSimilarity(leftList.get(leftIndex), rightList.get(rightIndex), context));
            }
            sum += max;
        }
        return CutoffSimiliarityMeasure.cutoff(sum / leftCollection.size(), this.cutoff);
    }

    @Override
    public SimilarityMeasure<C> cutoff(final float threshold) {
        return new MongeElkan<>(this.pairMeasure, this.maxPositionDiff, threshold);
    }
}
