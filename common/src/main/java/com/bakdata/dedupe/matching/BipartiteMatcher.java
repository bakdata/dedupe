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

import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.NonNull;

/**
 * Implements an algorithm that finds a matching in a bipartite graph.
 * <p>"A matching or independent edge set in a graph is a set of edges without common vertices."[1]</p>
 * <p>Unless explicitly stated otherwise by the implementation, the two types of vertexes may be of different size and
 * edges do not need to be complete; that is, certain matches may be impossible from the start. Edges may be negatively
 * weighted if the implementation supports it and it is up to the implementation to either see them as discouraged or
 * impossible, where non-existing edges should always be treated as impossible. </p>
 *
 * <p>[1]
 * <a href="https://en.wikipedia.org/wiki/Matching_(graph_theory)">https://en.wikipedia.org/wiki/Matching_(graph_theory)</a></p>
 *
 * @param <T> the type of the record
 * @see Assigner Assigner - a matcher on a bipartite graph with undirected edges
 */
@FunctionalInterface
public interface BipartiteMatcher<T> extends Assigner<T> {
    /**
     * Finds the set of edges forming a matching that maximizes the sum of the edge weights.
     * <p>Vertexes are implicitly given through the edges. Vertexes without any edge cannot be passed and are ignored.
     * It's up to the caller to handle such cases.</p>
     *
     * @param leftToRightWeightedEdges the set of possible edges coming from the left set of vertexes to the right ones
     * and their respective weights.
     * @param rightToLeftWeightedEdges the set of possible edges from the right set of vertexes to the left ones and
     * their respective weights.
     * @return the set of edges as a subset of leftToRightWeightedEdges.
     * @apiNote The interface may use Iterables as the parameters in the future to facilitate an online applications.
     * Use {@link #matchMaterialized(Collection, Collection)} if you need an explicit {@link Set}.
     */
    @NonNull Iterable<@NonNull ? extends WeightedEdge<T>> match(
            @NonNull Collection<WeightedEdge<T>> leftToRightWeightedEdges,
            @NonNull Collection<WeightedEdge<T>> rightToLeftWeightedEdges);

    /**
     * Finds the set of edges forming a matching that maximizes the sum of the edge weights.
     * <p>Vertexes are implicitly given through the edges. Vertexes without any edge cannot be passed and are ignored.
     * It's up to the caller to handle such cases.</p>
     *
     * @param leftToRightWeightedEdges the set of possible edges coming from the left set of vertexes to the right ones
     * and their respective weights.
     * @param rightToLeftWeightedEdges the set of possible edges from the right set of vertexes to the left ones and
     * their respective weights.
     * @return the set of edges as a subset of leftToRightWeightedEdges.
     */
    default @NonNull Set<@NonNull ? extends WeightedEdge<T>> matchMaterialized(
            @NonNull Collection<WeightedEdge<T>> leftToRightWeightedEdges,
            @NonNull Collection<WeightedEdge<T>> rightToLeftWeightedEdges) {
        return Sets.newHashSet(match(leftToRightWeightedEdges, rightToLeftWeightedEdges));
    }

    /**
     * @implNote Invokes {@link #match(Collection, Collection)} by replicating the given weighted edges for both
     * directions.
     */
    @Override
    default @NonNull Iterable<? extends WeightedEdge<T>> assign(
            @NonNull Collection<@NonNull WeightedEdge<T>> weightedEdges) {
        return match(weightedEdges, weightedEdges.stream().map(WeightedEdge::reversed).collect(Collectors.toList()));
    }
}
