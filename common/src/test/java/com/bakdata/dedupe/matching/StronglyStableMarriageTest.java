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

package com.bakdata.dedupe.matching;

import static com.bakdata.dedupe.matching.AbstractStableMarriage.AbstractMatcher.DUMMY_WEIGHT;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.dedupe.matching.StronglyStableMarriage.CriticalSetFinder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StronglyStableMarriageTest {
    @Nested
    class MatchingWithRanking {
        @Test
        void shouldFindStableMatches() {
            List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    createRanking(1, 0, 2, 3),
                    createRanking(3, 0, 1, 2),
                    createRanking(0, 2, 1, 3),
                    createRanking(1, 2, 0, 3)
            );
            List<Queue<List<Integer>>> womensRankingForMen = asList(
                    createRanking(0, 2, 1, 3),
                    createRanking(2, 3, 0, 1),
                    createRanking(3, 1, 2, 0),
                    createRanking(2, 1, 0, 3));

            final Stream<WeightedEdge<Integer>> matches =
                    new StronglyStableMarriage.Matcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 0, DUMMY_WEIGHT),
                            new WeightedEdge(1, 3, DUMMY_WEIGHT),
                            new WeightedEdge(2, 2, DUMMY_WEIGHT),
                            new WeightedEdge(3, 1, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatches2() {
            List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    createRanking(3, 0, 1, 2),
                    createRanking(1, 2, 0, 3),
                    createRanking(1, 3, 2, 0),
                    createRanking(2, 0, 3, 1)
            );
            List<Queue<List<Integer>>> womensRankingForMen = asList(
                    createRanking(3, 0, 2, 1),
                    createRanking(0, 2, 1, 3),
                    createRanking(0, 1, 2, 3),
                    createRanking(3, 0, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new StronglyStableMarriage.Matcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(1, 2, DUMMY_WEIGHT),
                            new WeightedEdge(2, 1, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatchesWithUnacceptablePartners() {
            List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    createRanking(3, 0, 2),
                    createRanking(1, 0, 3),
                    createRanking(1, 3, 2),
                    createRanking(0, 3, 1)
            );
            List<Queue<List<Integer>>> womensRankingForMen = asList(
                    createRanking(3, 0, 1),
                    createRanking(2, 1, 3),
                    createRanking(0, 2),
                    createRanking(0, 3, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new StronglyStableMarriage.Matcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(2, 1, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatchesWithTies() {
            List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    createRanking(3, 0, 1, 2),
                    createRanking(1, 2, 0, 3),
                    createRanking(1, 3, 2, 0),
                    createRanking(2, 0, 3, 1)
            );
            List<Queue<List<Integer>>> womensRankingForMen = asList(
                    createRanking(3, 0, 2, 1),
                    createRanking(new Integer[][]{{0}, {2, 1}, {3}}),
                    createRanking(0, 1, 2, 3),
                    createRanking(3, 0, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new StronglyStableMarriage.Matcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            // new Match(1, 2), new Match(2, 1) is only weakly stable
            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(1, 2, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        private Queue<List<Integer>> createRanking(int... favorites) {
            return new LinkedList<>(
                    IntStream.of(favorites).mapToObj(fav -> Lists.newArrayList(fav)).collect(Collectors.toList()));
        }

        private Queue<List<Integer>> createRanking(Integer[][] favorites) {
            return new LinkedList<>(
                    Stream.of(favorites).map(fav -> Lists.newArrayList(fav)).collect(Collectors.toList()));
        }
    }

    @Nested
    class CriticalSet {

        @Test
        void minimal() {
            Table<Integer, Integer, Boolean> engagements = HashBasedTable.create();
            engagements.put(0, 11, true);
            engagements.put(1, 13, true);
            engagements.put(2, 10, true);
            engagements.put(3, 11, true);

            final Set<Integer> men = new CriticalSetFinder(engagements).findMenInCriticalSubset();

            assertThat(men).containsExactlyInAnyOrder(0, 3);
        }

        @Test
        void medium() {
            Table<Integer, Integer, Boolean> engagements = HashBasedTable.create();
            engagements.put(0, 10, true);
            engagements.put(0, 11, true);
            engagements.put(0, 13, true);
            engagements.put(1, 11, true);
            engagements.put(1, 12, true);
            engagements.put(2, 11, true);
            engagements.put(2, 12, true);
            engagements.put(3, 11, true);
            engagements.put(3, 12, true);

            final Set<Integer> men = new CriticalSetFinder(engagements).findMenInCriticalSubset();

            assertThat(men).containsExactlyInAnyOrder(1, 2, 3);
        }

        @Test
        void max() {
            Table<Integer, Integer, Boolean> engagements = HashBasedTable.create();
            engagements.put(0, 10, true);
            engagements.put(0, 11, true);
            engagements.put(0, 13, true);
            engagements.put(1, 11, true);
            engagements.put(1, 12, true);
            engagements.put(2, 11, true);
            engagements.put(2, 12, true);
            engagements.put(3, 11, true);
            engagements.put(3, 12, true);
            engagements.put(4, 13, true);
            engagements.put(4, 14, true);

            final Set<Integer> men = new CriticalSetFinder(engagements).findMenInCriticalSubset();

            assertThat(men).containsExactlyInAnyOrder(1, 2, 3);
        }
    }
}
