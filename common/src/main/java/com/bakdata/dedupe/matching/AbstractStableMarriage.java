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

import static java.util.Comparator.comparingDouble;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AbstractStableMarriage<T> implements BipartiteMatcher<T> {
    @Override
    public Iterable<WeightedEdge<T>> match(final @NonNull Collection<WeightedEdge<T>> leftToRightWeights,
            final @NonNull Collection<WeightedEdge<T>> rightToLeftWeights) {
        final List<T> men = leftToRightWeights.stream().map(WeightedEdge::getFirst).distinct().collect(toList());
        final List<T> women = rightToLeftWeights.stream().map(WeightedEdge::getFirst).distinct().collect(toList());

        final List<Queue<List<Integer>>> mensFavoriteWomen = this.getRanking(leftToRightWeights, men, women);
        final List<Queue<List<Integer>>> womensFavoriteMen = this.getRanking(rightToLeftWeights, women, men);

        return this.createMatcher(mensFavoriteWomen, womensFavoriteMen).getStableMatches()
                .map(match -> {
                    final T m = men.get(match.getFirst());
                    final T w = women.get(match.getSecond());
                    return new WeightedEdge<>(m, w, this.getWeight(leftToRightWeights, m, w));
                })
                .collect(toList());
    }

    private double getWeight(final Collection<WeightedEdge<T>> leftScoreOfRight, final T m, final T w) {
        return leftScoreOfRight.stream()
                .filter(edge -> edge.getFirst().equals(m) &&
                        edge.getSecond().equals(w))
                .findFirst()
                .orElseThrow().getWeight();
    }

    protected abstract @NonNull Matcher createMatcher(List<? extends Queue<List<Integer>>> mensFavoriteWomen,
            List<? extends Queue<List<Integer>>> womensFavoriteMen);

    /**
     * Returns the ranking defined by the source for the target. Dimensions:
     * <ul>
     * <li>Outer list indexes all source entries.</li>
     * <li>Queue shows the preferences. First elements are highest ranked.</li>
     * <li>Inner list groups all equally ranked targets.</li>
     * </ul>
     */
    private List<Queue<List<Integer>>> getRanking(final Iterable<WeightedEdge<T>> weightedEdges,
            final @NonNull List<T> sources,
            final @NonNull List<T> targets) {
        final Table<T, T, Double> scores = HashBasedTable.create();
        for (final WeightedEdge<T> weightedEdge : weightedEdges) {
            scores.put(weightedEdge.getFirst(), weightedEdge.getSecond(), weightedEdge.getWeight());
        }

        return sources.stream().map(source -> {
            final Map<Double, List<Integer>> scoreGroup = IntStream.range(0, targets.size()).boxed()
                    // get the score for a given target
                    .map(targetIndex -> Pair.of(targetIndex,
                            scores.get(source, targets.get(targetIndex))))
                    // filter unranked targets
                    .filter(targetScore -> targetScore.getRight() != null)
                    // group by score
                    .collect(groupingBy(Pair::getRight, mapping(Pair::getLeft, toList())));
            // sort groups by highest score
            return scoreGroup
                    .entrySet()
                    .stream()
                    .sorted(comparingDouble(group -> -group.getKey()))
                    .map(Entry::getValue)
                    .collect(toCollection(LinkedList::new));
        }).collect(toList());
    }

    @FunctionalInterface
    protected interface Matcher {
        Stream<WeightedEdge<Integer>> getStableMatches();
    }

    protected abstract static class AbstractMatcher implements Matcher {
        static final int DUMMY_WEIGHT = 1;
        protected final List<? extends Queue<List<Integer>>> mensFavoriteWomen;
        protected final List<? extends Queue<List<Integer>>> womensFavoriteMen;

        // engagements row = man, col = woman
        protected final @NonNull Table<Integer, Integer, Boolean> engagements;

        protected AbstractMatcher(final List<? extends Queue<List<Integer>>> mensFavoriteWomen,
                final List<? extends Queue<List<Integer>>> womensFavoriteMen) {
            this.mensFavoriteWomen = mensFavoriteWomen;
            this.womensFavoriteMen = womensFavoriteMen;
            this.engagements = HashBasedTable.create();
        }

        protected static List<Integer> getStrictSuccessors(final @NonNull Queue<List<Integer>> favoriteMen,
                final Integer m) {
            return getTailStream(favoriteMen, m).skip(1).flatMap(Collection::stream)
                    .collect(toList());
        }

        protected static List<Integer> getSuccessors(final @NonNull Queue<List<Integer>> favoriteMen,
                final @NonNull Integer m) {
            return getTailStream(favoriteMen, m).flatMap(Collection::stream)
                    .dropWhile(m2 -> !m.equals(m2))
                    .skip(1)
                    .collect(toList());
        }

        protected static List<Integer> getTail(final @NonNull Queue<List<Integer>> favoriteMen, final Integer m) {
            return getTailStream(favoriteMen, m).flatMap(Collection::stream).collect(toList());
        }

        private static Stream<List<Integer>> getTailStream(final Collection<List<Integer>> favoriteMen,
                final Integer m) {
            return favoriteMen.stream().dropWhile(equallyGoodMen -> !equallyGoodMen.contains(m));
        }

        private static void deleteInFav(final Iterable<List<Integer>> favs, final Integer elem) {
            final Iterator<List<Integer>> iterator = favs.iterator();
            while (iterator.hasNext()) {
                final List<Integer> equallyGood = iterator.next();
                if (equallyGood.remove(elem)) {
                    if (equallyGood.isEmpty()) {
                        iterator.remove();
                    }
                    break;
                }
            }
        }

        protected Integer getNextFreeMen() {
            for (int m = 0; m < this.mensFavoriteWomen.size(); m++) {
                if (this.engagements.row(m).isEmpty() && !this.mensFavoriteWomen.get(m).isEmpty()) {
                    return m;
                }
            }
            return null;
        }

        protected void propose(final @NonNull Integer m, final @NonNull Integer w) {
            this.engagements.put(m, w, true);
        }

        @Override
        public Stream<WeightedEdge<Integer>> getStableMatches() {
            this.match();
            return this.engagements.cellSet().stream()
                    .filter(cell -> cell.getValue() != null)
                    .map(cell -> {
                        final Integer first = cell.getRowKey();
                        final Integer second = cell.getColumnKey();
                        return new WeightedEdge<>(first, second, DUMMY_WEIGHT);
                    });
        }

        protected abstract void match();

        protected void breakEngangement(final Integer m, final Integer w) {
            this.engagements.remove(m, w);
        }

        protected void delete(final Integer m, final Integer w) {
            AbstractMatcher.deleteInFav(this.mensFavoriteWomen.get(m), w);
            AbstractMatcher.deleteInFav(this.womensFavoriteMen.get(w), m);
        }

    }
}
