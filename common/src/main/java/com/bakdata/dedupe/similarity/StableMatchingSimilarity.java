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
import lombok.NonNull;
import lombok.Value;

@Value
public class StableMatchingSimilarity<C extends Collection<? extends T>, T>
        implements CollectionSimilarityMeasure<C, T> {
//    private final Assigner<T> stableAssignment;

    public StableMatchingSimilarity(final SimilarityMeasure<T> pairMeasure) {
//        this.stableAssignment = new StableMarriage<>(pairMeasure);
    }

    @Override
    public float calculateNonEmptyCollectionSimilarity(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
//        final Collection<Match<T>> matches =
//                stableAssignment.getMatches(List.copyOf(leftCollection), List.copyOf(rightCollection), context);
//        return matches.stream().map(Match::getSimilarity).reduce(0f, Float::sum) /
//                Math.max(leftCollection.size(), rightCollection.size());
        return 0;
    }
}
