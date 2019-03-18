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

import com.bakdata.dedupe.matching.BipartiteMatcher;
import java.util.Collection;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 * Monge-Elkan is a simple list-based similarity measure, where elements from the left are matched with elements from
 * the right with the highest similarity within a certain index range.
 * <p>It is simple in the regard that the same element on the right side can be matched multiple times to left
 * elements.</p>
 * <p>For non-repeating matching, consider {@link CommonSimilarityMeasures#matching(BipartiteMatcher,
 * SimilarityMeasure)}.</p>
 * <p>However, it is comparably fast, as only the similarities within the neighborhood are calculated. For a larger
 * neighborhood, the matching approach is usually preferable.</p>
 * <p>Monge-Elkan prefers the left side over the right side.</p>
 * <p>Note that Monge-Elkan distance can be well sped up by using a threshold (e.g.,
 * {@link SimilarityMeasure#cutoff(double)} or {@link SimilarityMeasure#scaleWithThreshold(double)}).</p>
 *
 * @param <E> the element type.
 * @param <C> the collection type.
 */
@Value
class MongeElkan<C extends Collection<? extends E>, E> implements CollectionSimilarityMeasure<C, E> {
    /**
     * The similarity measure to use to calculate the preferences and the overall similarity.
     */
    SimilarityMeasure<E> pairMeasure;
    /**
     * The maximum index difference of the left list item and the right list item.
     */
    int maxPositionDiff;
    /**
     * The cutoff value, which is used to prematurely terminate the calculation.
     */
    @Getter(AccessLevel.PRIVATE)
    double cutoff;

    @Override
    public double calculateNonEmptyCollectionSimilarity(@NonNull final C leftCollection,
            @NonNull final C rightCollection, @NonNull final SimilarityContext context) {
        final List<E> leftList = List.copyOf(leftCollection);
        final List<E> rightList = List.copyOf(rightCollection);
        // consider a cutoff of .9 and |left| = 3, then on average each element has .1 buffer
        // as soon as the current sum + buffer < index, the cutoff threshold cannot be passed (buffer used up)
        final double cutoffBuffer = (1 - this.cutoff) * leftCollection.size();
        double sum = 0;
        for (int leftIndex = 0; leftIndex < leftCollection.size() && (cutoffBuffer + sum) >= leftIndex;
                leftIndex++) {
            double max = 0;
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
    public SimilarityMeasure<C> cutoff(final double threshold) {
        return new MongeElkan<>(this.pairMeasure, this.maxPositionDiff, threshold);
    }
}
