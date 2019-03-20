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

import com.bakdata.dedupe.matching.WeaklyStableMarriage.WeakMatcher;
import com.google.common.collect.Lists;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class WeaklyStableMarriageTest {

    @Nested
    class MatchingWithRanking {
        @Test
        void shouldFindStableMatches() {
            final List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    this.createRanking(1, 0, 2, 3),
                    this.createRanking(3, 0, 1, 2),
                    this.createRanking(0, 2, 1, 3),
                    this.createRanking(1, 2, 0, 3)
            );
            final List<Queue<List<Integer>>> womensRankingForMen = asList(
                    this.createRanking(0, 2, 1, 3),
                    this.createRanking(2, 3, 0, 1),
                    this.createRanking(3, 1, 2, 0),
                    this.createRanking(2, 1, 0, 3));

            final Stream<WeightedEdge<Integer>> matches =
                    new WeakMatcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 0, DUMMY_WEIGHT),
                            new WeightedEdge(1, 3, DUMMY_WEIGHT),
                            new WeightedEdge(2, 2, DUMMY_WEIGHT),
                            new WeightedEdge(3, 1, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatches2() {
            final List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    this.createRanking(3, 0, 1, 2),
                    this.createRanking(1, 2, 0, 3),
                    this.createRanking(1, 3, 2, 0),
                    this.createRanking(2, 0, 3, 1)
            );
            final List<Queue<List<Integer>>> womensRankingForMen = asList(
                    this.createRanking(3, 0, 2, 1),
                    this.createRanking(0, 2, 1, 3),
                    this.createRanking(0, 1, 2, 3),
                    this.createRanking(3, 0, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new WeakMatcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(1, 2, DUMMY_WEIGHT),
                            new WeightedEdge(2, 1, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatchesWithUnacceptablePartners() {
            final List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    this.createRanking(3, 0, 2),
                    this.createRanking(1, 0, 3),
                    this.createRanking(1, 3, 2),
                    this.createRanking(0, 3, 1)
            );
            final List<Queue<List<Integer>>> womensRankingForMen = asList(
                    this.createRanking(3, 0, 1),
                    this.createRanking(2, 1, 3),
                    this.createRanking(0, 2),
                    this.createRanking(0, 3, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new WeakMatcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(2, 1, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        @Test
        void shouldFindStableMatchesWithTies() {
            final List<Queue<List<Integer>>> mensFavoriteWomen = asList(
                    this.createRanking(3, 0, 1, 2),
                    this.createRanking(3, 1, 2, 0),
                    this.createRanking(1, 3, 2, 0),
                    this.createRanking(2, 0, 3, 1)
            );
            final List<Queue<List<Integer>>> womensRankingForMen = asList(
                    this.createRanking(3, 0, 2, 1),
                    this.createRanking(new Integer[][]{{0}, {2, 1}, {3}}),
                    this.createRanking(0, 1, 2, 3),
                    this.createRanking(3, 0, 2, 1));

            final Stream<WeightedEdge<Integer>> matches =
                    new WeakMatcher(mensFavoriteWomen, womensRankingForMen).getStableMatches();

            assertThat(matches.collect(Collectors.toList()))
                    .containsExactlyInAnyOrder(new WeightedEdge(0, 3, DUMMY_WEIGHT),
                            new WeightedEdge(1, 2, DUMMY_WEIGHT),
                            new WeightedEdge(2, 1, DUMMY_WEIGHT),
                            new WeightedEdge(3, 0, DUMMY_WEIGHT));
        }

        private Queue<List<Integer>> createRanking(final int... favorites) {
            return new LinkedList<>(
                    IntStream.of(favorites).mapToObj(fav -> Lists.newArrayList(fav)).collect(Collectors.toList()));
        }

        private Queue<List<Integer>> createRanking(final Integer[][] favorites) {
            return new LinkedList<>(
                    Stream.of(favorites).map(fav -> Lists.newArrayList(fav)).collect(Collectors.toList()));
        }
    }
}
