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
package com.bakdata.dedupe.clustering;

import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.NonNull;

/**
 * A clustering algorithm takes a list of {@link ClassifiedCandidate}s and creates a coherent {@link Cluster}, such that
 * all pairs of records inside the cluster are duplicate and no record outside the cluster is a duplicate with any
 * record inside the cluster. This requirement implies that {@link com.bakdata.dedupe.classifier.Classification}s are
 * additionally performed, reevaluated, or discarded to obtain the necessary coherence.
 *
 * @implSpec A clustering which also splits previously outputted clusters should give users a chance to react to the
 * changes with a {@link ClusterSplitHandler}. Implementations may decide to adhere to the non-splitting wish or not.
 */
public interface Clustering<C extends Comparable<C>, T, I> {
    /**
     * Creates a coherent {@link Cluster} from a list of {@link ClassifiedCandidate}s.
     *
     * @param classifiedCandidates the list of classified candidates.
     * @return a coherent cluster over the classified candidates.
     */
    @NonNull Stream<Cluster<C, T>> cluster(@NonNull Stream<ClassifiedCandidate<T>> classifiedCandidates);

    /**
     * The cluster id generator that is used to create an id for a new cluster.
     *
     * @return the cluster id generator.
     */
    @NonNull Function<? super Iterable<? extends I>, C> getClusterIdGenerator();
}
