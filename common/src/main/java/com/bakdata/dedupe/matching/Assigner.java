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
import lombok.NonNull;

/**
 * Implements an algorithm that solves an assignment problem.
 * <p>"It consists of finding, in a [undirected], weighted bipartite graph, a matching in which the sum of weights of
 * the edges is as large as possible."[1]</p>
 * <p>Unless explicitly stated otherwise by the implementation, the two types of vertexes may be of different size and
 * edges do not need to be complete; that is, certain assignments may be impossible from the start. Edges may be
 * negatively weighted if the implementation supports it and it is up to the implementation to either see them as
 * discouraged or impossible, where non-existing edges should always be treated as impossible. </p>
 * <p>A generalization of Assigner is the {@link BipartiteMatcher} that operators on directed, weighted bipartite graphs.</p>
 *
 * <p>[1] <a href="https://en.wikipedia.org/wiki/Assignment_problem">https://en.wikipedia.org/wiki/Assignment_problem</a></p>
 *
 * @param <T> the type of the record
 * @see BipartiteMatcher
 */
@FunctionalInterface
public interface Assigner<T> {
    /**
     * Finds the set of edges forming a matching that maximizes the sum of the edge weights.
     * <p>Vertexes are implicitly given through the edges. Vertexes without any edge cannot be passed and are ignored.
     * It's up to the caller to handle such cases.</p>
     *
     * @param weightedEdges the set of possible edges and their respective weights.
     * @return the set of edges.
     * @apiNote The interface may use Iterables as the parameters in the future to facilitate an online applications.
     * Use {@link #assignMaterialized(Collection)} if you need an explicit {@link Set}.
     */
    @NonNull Iterable<@NonNull ? extends Match<T>> assign(@NonNull Collection<@NonNull WeightedEdge<T>> weightedEdges);

    /**
     * Finds the set of edges forming a matching that maximizes the sum of the edge weights.
     * <p>Vertexes are implicitly given through the edges. Vertexes without any edge cannot be passed and are ignored.
     * It's up to the caller to handle such cases.</p>
     *
     * @param weightedEdges the set of possible edges and their respective weights.
     * @return the set of edges.
     */
    default @NonNull Set<@NonNull ? extends Match<T>> assignMaterialized(
            @NonNull Collection<@NonNull WeightedEdge<T>> weightedEdges) {
        return Sets.newHashSet(assign(weightedEdges));
    }
}
