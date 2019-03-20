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

import static com.bakdata.dedupe.similarity.SimilarityMeasure.isUnknown;

import com.bakdata.dedupe.matching.BipartiteMatcher;
import com.bakdata.dedupe.matching.WeightedEdge;
import com.bakdata.util.StreamUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;


/**
 * A similarity measure that finds the <i>best</i> matches between two bags of entities and calculates an overall
 * similarity by summing the similarity of these matches and normalize it over the (max) number of elements per
 * collection.
 */
@Value
public class MatchingSimilarity<C extends Collection<? extends E>, E> implements CollectionSimilarityMeasure<C, E> {
    @NonNull BipartiteMatcher<E> bipartiteMatcher;
    @NonNull SimilarityMeasure<E> pairMeasure;

    @Override
    public double calculateNonEmptyCollectionSimilarity(final @NonNull C leftCollection, final @NonNull C rightCollection,
            final @NonNull SimilarityContext context) {
        final Collection<WeightedEdge<E>> leftScoreOfRight = this.getScores(leftCollection, rightCollection, context);
        final Collection<WeightedEdge<E>> rightScoreOfLeft = this.pairMeasure.isSymmetric()
                ? this.reversed(leftScoreOfRight)
                : this.getScores(rightCollection, leftCollection, context);

        final Iterable<? extends WeightedEdge<E>> matches = this.bipartiteMatcher
                .match(leftScoreOfRight, rightScoreOfLeft);
        return StreamUtil.stream(matches)
                       .map(WeightedEdge::getWeight)
                       .reduce(0d, Double::sum) /
               Math.max(leftCollection.size(), rightCollection.size());
    }

    private Collection<WeightedEdge<E>> reversed(final Collection<WeightedEdge<E>> scores) {
        return scores.stream().map(score -> score.reversed()).collect(Collectors.toList());
    }

    @NonNull
    private Collection<WeightedEdge<E>> getScores(final @NonNull C leftCollection, final @NonNull C rightCollection,
            final @NonNull SimilarityContext context) {
        final Collection<WeightedEdge<E>> leftScoreOfRight = new ArrayList<>();
        for (final E left : leftCollection) {
            for (final E right : rightCollection) {
                final double leftScore = this.pairMeasure.getSimilarity(left, right, context);
                if (!isUnknown(leftScore)) {
                    leftScoreOfRight.add(new WeightedEdge<>(left, right, leftScore));
                }
            }
        }
        return leftScoreOfRight;
    }
}
