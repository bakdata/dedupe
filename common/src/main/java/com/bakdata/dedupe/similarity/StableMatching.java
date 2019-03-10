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

import static java.util.Comparator.comparingDouble;

import com.google.common.annotations.VisibleForTesting;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.Value;

@Value
public class StableMatching<T> {
    private final SimilarityMeasure<T> pairMeasure;

    @VisibleForTesting
    static Map<Integer, Integer> getStableMatches(final List<? extends Queue<Integer>> mensFavoriteWomen,
            final List<? extends Map<Integer, Integer>> womensRankingForMen) {
        final Map<Integer, Integer> womenEngagements = new HashMap<>();
        // Initialize all m ∈ M and w ∈ W to free
        final Deque<Integer> unassignedMen =
                IntStream.range(0, mensFavoriteWomen.size()).boxed().collect(Collectors.toCollection(LinkedList::new));
        // while ∃ free man m who still has a woman w to propose to {
        while (!unassignedMen.isEmpty()) {
            final int freeMan = unassignedMen.remove();
            final Queue<Integer> womanToWhichToPropose = mensFavoriteWomen.get(freeMan);
            if (!womanToWhichToPropose.isEmpty()) {
                // w = first woman on m’s list to whom m has not yet proposed
                final Integer woman = womanToWhichToPropose.poll();
                final Integer currentMan = womenEngagements.get(woman);
                // if w is free
                if (currentMan == null) {
                    // (m, w) become engaged
                    womenEngagements.put(woman, freeMan);
                    // else some pair (m', w) already exists
                } else {
                    final Map<Integer, Integer> ranking = womensRankingForMen.get(woman);
                    // if w prefers m to m'
                    if (ranking.get(freeMan) < ranking.get(currentMan)) {
                        // m' becomes free
                        unassignedMen.add(currentMan);
                        // (m, w) become engaged
                        womenEngagements.put(woman, freeMan);
                    } else {
                        // re-add current men. Note, only happens when there are women, to which to propose, are left
                        unassignedMen.addFirst(freeMan);
                    }
                }
            }
        }
        return womenEngagements;
    }

    public List<Match<T>> getMatches(@NonNull final List<? extends T> men,
            @NonNull final List<? extends T> women, @NonNull final SimilarityContext context) {
        final List<List<Float>> similarities = getSimilarityMatrix(men, women, context);

        final List<LinkedList<Integer>> mensFavoriteWomen = getFavorites(men, women, similarities);
        final List<Map<Integer, Integer>> womensRankingForMen = getWomensRankingForMen(men, women, context);

        final Map<Integer, Integer> womenEngagements = getStableMatches(mensFavoriteWomen, womensRankingForMen);
        return womenEngagements.entrySet().stream()
                .map(engagement -> new Match<>(men.get(engagement.getValue()),
                        women.get(engagement.getKey()),
                        similarities.get(engagement.getValue()).get(engagement.getKey())))
                .collect(Collectors.toList());
    }

    private List<Map<Integer, Integer>> getWomensRankingForMen(@NonNull final List<? extends T> men,
            @NonNull final List<? extends T> women, final @NonNull SimilarityContext context) {
        final List<List<Float>> similarities = getSimilarityMatrix(women, men, context);
        final List<LinkedList<Integer>> favorites = getFavorites(women, men, similarities);

        return favorites.stream()
                .map(preferences -> IntStream.range(0, preferences.size())
                        .boxed()
                        .collect(Collectors.toMap(preferences::get, Function.identity())))
                .collect(Collectors.toList());
    }

    private List<LinkedList<Integer>> getFavorites(@NonNull final List<? extends T> men,
            @NonNull final List<? extends T> women, final List<List<Float>> similarities) {
        return IntStream.range(0, men.size()).mapToObj(manIndex ->
                IntStream.range(0, women.size()).boxed()
                        .sorted(comparingDouble(womanIndex -> -similarities.get(manIndex).get(womanIndex)))
                        .collect(Collectors.toCollection(LinkedList::new))
        ).collect(Collectors.toList());
    }

    private List<List<Float>> getSimilarityMatrix(@NonNull final List<? extends T> men,
            @NonNull final List<? extends T> women,
            @NonNull final SimilarityContext context) {
        return IntStream.range(0, men.size()).mapToObj(manIndex ->
                IntStream.range(0, women.size()).mapToObj(womanIndex ->
                        pairMeasure.getSimilarity(men.get(manIndex), women.get(womanIndex), context)
                ).collect(Collectors.toList())
        ).collect(Collectors.toList());
    }

    @Value
    public static class Match<T> {
        T left;
        T right;
        float similarity;
    }
}
