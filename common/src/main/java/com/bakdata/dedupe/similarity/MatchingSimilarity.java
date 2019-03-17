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
import com.bakdata.dedupe.matching.MatchMaker;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import java.util.Collection;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.Value;

@Value
public class MatchingSimilarity<C extends Collection<? extends T>, T> implements CollectionSimilarityMeasure<C, T> {
    @NonNull MatchMaker<T> matchMaker;
    @NonNull SimilarityMeasure<T> pairMeasure;

    @Override
    public float calculateNonEmptyCollectionSimilarity(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
        final Table<T, T, Float> leftScoreOfRight = getScores(leftCollection, rightCollection, context);
        final Table<T, T, Float> rightScoreOfLeft = pairMeasure.isSymmetric()
                ? Tables.transpose(leftScoreOfRight)
                : getScores(rightCollection, leftCollection, context);

        final Iterable<? extends Match<T>> matches = matchMaker.match(leftScoreOfRight, rightScoreOfLeft);
        return StreamSupport.stream(matches.spliterator(), false)
                       .map(match -> leftScoreOfRight.get(match.getLeft(), match.getRight()))
                       .reduce(0f, Float::sum) /
               Math.max(leftCollection.size(), rightCollection.size());
    }

    private Table<T, T, Float> getScores(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
        final Table<T, T, Float> leftScoreOfRight = HashBasedTable.create();
        for (T left : leftCollection) {
            for (T right : rightCollection) {
                final float leftScore = pairMeasure.getSimilarity(left, right, context);
                if (!isUnknown(leftScore)) {
                    leftScoreOfRight.put(left, right, leftScore);
                }
            }
        }
        return leftScoreOfRight;
    }
}
