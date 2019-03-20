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
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jgrapht.Graph;
import org.jgrapht.alg.interfaces.MatchingAlgorithm.Matching;
import org.jgrapht.alg.matching.HopcroftKarpMaximumCardinalityBipartiteMatching;
import org.jgrapht.graph.SimpleGraph;

/**
 * Implements a strongly stable matching based on the stable marriage with indifference.
 * <p>"A matching is strongly stable if there is no couple x, y such that x strictly prefers y to his/her partner, and
 * y either strictly prefers x to his/her partner or is indifferent between them."[1]</p>
 * <p>The preference list may be incomplete if some pairs absolutely do not want to matched. However, in this case,
 * there may be fewer matches than elements.</p>
 *
 * <p>[1] Irving, Robert W. (1994-02-15). "Stable marriage and indifference". Discrete Applied Mathematics. 48 (3):
 * 261–272. doi:10.1016/0166-218X(92)00179-P.</p>
 *
 * @param <T> the type of the record.
 */
@Value
@EqualsAndHashCode(callSuper = false)
public class StronglyStableMarriage<T> extends AbstractStableMarriage<T> {
    @Override
    protected AbstractStableMarriage.Matcher createMatcher(final List<? extends Queue<List<Integer>>> mensFavoriteWomen,
            final List<? extends Queue<List<Integer>>> womensFavoriteMen) {
        return new Matcher(mensFavoriteWomen, womensFavoriteMen);
    }

    /**
     * Finds the critical set of men that cannot be matched in the current match step. Irving only discussed the
     * existence of this set[1] but not the calculation, so we developed our own algorithm.
     * <p>The input of this algorithm are all current proposals. The strongly stable algorithm allows that the same
     * women temporarily accepts multiple offers. </p>
     *
     * <p>Basic idea is that the maximum matching of a given match step will lack at least one men that is part of the
     * critical set. By removing this men and the neighbors (at least one women), we will find additional men in the
     * critical set and continue to remove the neighboring women. Repeat until no more men is deemed critical; that is,
     * the maximum matching contains all remaining men.</p>
     *
     * <p>The complexity depends on the maximum matching algorithm, which is currently the Hopcroft Karp matching
     * algorithm and has a complexity of $O(|E| \cdot \sqrt{|V|})$. It is invoked at most $n$ times as with each
     * invocation at least one man is removed. $E$ is the matching of the current matching step and thus bound by V.
     * Thus the overall complexity is: $O(|V|^2.5)$.</p>
     */
    static class CriticalSetFinder {
        private final Graph<Integer, WeightedEdge<Integer>> graph = new SimpleGraph<>(null, null, false);
        private Table<Integer, Integer, Boolean> engagements;
        private Set<Integer> manVertexes = new HashSet<>();
        private Set<Integer> womanVertexes = new HashSet<>();

        public CriticalSetFinder(final Table<Integer, Integer, Boolean> engagements) {
            this.engagements = engagements;
            createGraph();
        }

        @VisibleForTesting
        Set<Integer> findMenInCriticalSubset() {
            Set<Integer> criticalMen = new HashSet<>();

            while (true) {
                final Matching<Integer, WeightedEdge<Integer>> matching =
                        new HopcroftKarpMaximumCardinalityBipartiteMatching(graph,
                                manVertexes, womanVertexes).getMatching();

                // check if "perfect" matching; that is, all remaining men are matched.
                if (matching.getEdges().size() == manVertexes.size()) {
                    break;
                }

                // else at least one man did not get matched.
                Set<Integer> unmatchedMen = new HashSet<>(manVertexes);
                unmatchedMen.removeAll(matching.getEdges().stream().map(WeightedEdge::getFirst).collect(toSet()));

                // find neighboring women
                final Set<Integer> newCriticalWomen = unmatchedMen.stream().flatMap(m -> graph.edgesOf(m).stream().map(
                        match -> match.getSecond())).collect(toSet());

                // remove both and their edges
                for (Integer w : newCriticalWomen) {
                    graph.removeVertex(w);
                }
                for (Integer m : unmatchedMen) {
                    graph.removeVertex(m);
                }

                // also prune them from the input of the next iteration
                manVertexes.removeAll(unmatchedMen);
                womanVertexes.removeAll(newCriticalWomen);
                criticalMen.addAll(unmatchedMen);
            }

            return criticalMen;
        }

        /**
         * For each current engagement, add an edge into the graph.
         */
        private void createGraph() {
            for (Cell<Integer, Integer, Boolean> engagement : engagements.cellSet()) {
                if (engagement.getValue() != null) {
                    final Integer m = engagement.getRowKey();
                    final Integer w = engagement.getColumnKey();
                    manVertexes.add(m);
                    graph.addVertex(m);
                    womanVertexes.add(-w - 1);
                    graph.addVertex(-w - 1);
                    graph.addEdge(m, -w - 1, new WeightedEdge<>(m, -w - 1, DUMMY_WEIGHT));
                }
            }
        }
    }

    /**
     * Implements the strong stable matching of [1].
     */
    @VisibleForTesting
    static class Matcher extends AbstractStableMarriage.AbstractMatcher {
        Matcher(final List<? extends Queue<List<Integer>>> mensFavoriteWomen,
                final List<? extends Queue<List<Integer>>> womensFavoriteMen) {
            super(mensFavoriteWomen, womensFavoriteMen);
        }

        @Override
        protected void match() {
            // while (some man m is free) do
            Integer m;
            while ((m = getNextFreeMen()) != null) {
                final List<Integer> highestRankedWomen = mensFavoriteWomen.get(m).peek();
                //for each (woman w at the head of m's list) do
                for (Integer w : highestRankedWomen) {
                    //m proposes, and becomes engaged, to w;
                    propose(m, w);

                    //for each (strict successor m' of m on w’s list) do
                    getStrictSuccessors(womensFavoriteMen.get(w), m).forEach(m_ -> {
                        breakEngangement(m_, w);
                        delete(m_, w);
                    });
                }
                //find the critical set Z of men;
                Set<Integer> z = new CriticalSetFinder(engagements).findMenInCriticalSubset();
                // perfect matching if no critical set
                if (!z.isEmpty()) {
                    //for each (woman w who is engaged to a man in Z) do
                    for (Integer criticalMan : z) {
                        for (Integer w : engagements.row(criticalMan).keySet()) {
                            //break all engagements involving w;
                            engagements.column(w).clear();

                            //for each (tail m' of m on w’s list) do
                            getTail(womensFavoriteMen.get(w), m).forEach(m_ -> {
                                breakEngangement(m_, w);
                                delete(m_, w);
                            });
                        }
                    }
                }
            }
        }
    }
}