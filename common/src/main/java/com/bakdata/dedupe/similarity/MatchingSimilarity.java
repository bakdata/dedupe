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

import com.bakdata.dedupe.matching.Match;
import com.bakdata.dedupe.matching.BipartiteMatcher;
import com.bakdata.dedupe.matching.WeightedEdge;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.Value;

@Value
public class MatchingSimilarity<C extends Collection<? extends T>, T> implements CollectionSimilarityMeasure<C, T> {
    @NonNull BipartiteMatcher<T> bipartiteMatcher;
    @NonNull SimilarityMeasure<T> pairMeasure;

    @Override
    public float calculateNonEmptyCollectionSimilarity(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
        final Collection<WeightedEdge<T>> leftScoreOfRight = getScores(leftCollection, rightCollection, context);
        final Collection<WeightedEdge<T>> rightScoreOfLeft = pairMeasure.isSymmetric()
                ? reversed(leftScoreOfRight)
                : getScores(rightCollection, leftCollection, context);

        final Iterable<? extends Match<T>> matches = bipartiteMatcher.match(leftScoreOfRight, rightScoreOfLeft);
        return StreamSupport.stream(matches.spliterator(), false)
                       .map(match -> getWeight(leftScoreOfRight, match))
                       .reduce(0f, Float::sum) /
               Math.max(leftCollection.size(), rightCollection.size());
    }

    private float getWeight(Collection<WeightedEdge<T>> leftScoreOfRight, Match<T> match) {
        return leftScoreOfRight.stream()
                .filter(edge -> edge.getFirst().equals(match.getFirst()) &&
                                edge.getSecond().equals(match.getSecond()))
                .findFirst()
                .orElseThrow().getWeight();
    }

    private Collection<WeightedEdge<T>> reversed(Collection<WeightedEdge<T>> scores) {
        return scores.stream().map(score -> score.reversed()).collect(Collectors.toList());
    }

    private Collection<WeightedEdge<T>> getScores(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
        final Collection<WeightedEdge<T>> leftScoreOfRight = new ArrayList<>();
        for (T left : leftCollection) {
            for (T right : rightCollection) {
                final float leftScore = pairMeasure.getSimilarity(left, right, context);
                if (!isUnknown(leftScore)) {
                    leftScoreOfRight.add(new WeightedEdge<>(left, right, leftScore));
                }
            }
        }
        return leftScoreOfRight;
    }
}
