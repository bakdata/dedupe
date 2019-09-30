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

import com.bakdata.dedupe.candidate_selection.online.OnlineCandidate;
import com.bakdata.dedupe.classifier.ClassificationResult;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.bakdata.dedupe.classifier.Classifier;
import com.google.common.primitives.Bytes;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.experimental.Wither;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Splits large clusters into smaller clusters when the inter-cluster similarities are sub-optimal.
 * <p>{@link RefineCluster} can be applied after aggressive clustering variations (leaning towards recall), such as
 * {@link TransitiveClosure}, to boost the overall precision.</p>
 * <p>To determine the inner-cluster similarities the confidence score of a {@link Classifier} is used. Negative scores
 * are allowed and strongly discourage certain pairs.</p>
 * <p>If all scores within a cluster are positive, the cluster will remain intact.</p>
 * <p>Note, this algorithm implements a perfect clustering for small clusters and a heuristic for large clusters. Small
 * clusters are below {@link #maxSmallClusterSize}.</p>
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 */
@Value
@Builder
public class RefineCluster<C extends Comparable<C>, T> {
    private static final Random RANDOM = ThreadLocalRandom.current();

    /**
     * The maximum size (inclusive) of a cluster. This size limits the maximum amount of comparisons to {@code max *
     * (max - 1) / 2}.
     */
    @Builder.Default
    final int maxSmallClusterSize = 10;
    /**
     * The classifier used to score the edges. Please note that binary classifiers (confidence always 1) can be used but
     * will not unleash the full potential.
     */
    @NonNull
    Classifier<T> classifier;
    /**
     * A function to generate the id for newly split clusters.
     */
    @NonNull
    Function<? super Iterable<? extends T>, C> clusterIdGenerator;

    private static double getWeight(final ClassificationResult classificationResult) {
        switch (classificationResult.getClassification()) {
            case DUPLICATE:
                return classificationResult.getConfidence();
            case NON_DUPLICATE:
                return -classificationResult.getConfidence();
            case UNKNOWN:
                return -0.0d;
            default:
                throw new IllegalStateException();
        }
    }

    private static double scoreClustering(final byte[] partitions, final double[][] weightMatrix) {
        final int n = partitions.length;
        final int[] partitionSizes = new int[n];
        for (final byte clustering : partitions) {
            partitionSizes[clustering]++;
        }

        double score = 0;
        for (int rowIndex = 0; rowIndex < n; rowIndex++) {
            for (int colIndex = rowIndex + 1; colIndex < n; colIndex++) {
                final double weightForEdge = weightMatrix[rowIndex][colIndex];
                if (partitions[rowIndex] == partitions[colIndex]) {
                    score += weightForEdge / partitionSizes[partitions[rowIndex]];
                } else {
                    score -= weightForEdge / (n - partitionSizes[partitions[rowIndex]]) +
                            weightForEdge / (n - partitionSizes[partitions[colIndex]]);
                }
            }
        }
        return score;
    }

    static List<WeightedEdge> getRandomEdges(final int potentialNumEdges, final int desiredNumEdges) {
        return RANDOM.ints(0, potentialNumEdges)
                .distinct()
                .mapToObj(RefineCluster::createGaussPair)
                .filter(RefineCluster::isNotSelfPair)
                .map(p -> WeightedEdge.of(p.getLeft(), p.getRight(), Double.NaN))
                .limit(desiredNumEdges)
                .collect(Collectors.toList());
    }

    private static <T> boolean isNotSelfPair(final Pair<T, T> pair) {
        return !pair.getLeft().equals(pair.getRight());
    }

    static Pair<Integer, Integer> createGaussPair(final int i) {
        // reverse of Gaussian
        final int leftIndex = (int) (Math.sqrt(2 * i + 0.25) - 0.5);
        final int rightIndex = i - gaussianSum(leftIndex);
        return Pair.of(leftIndex, rightIndex);
    }

    static int gaussianSum(final int n) {
        return (n + 1) * (n) / 2;
    }

    private List<ClassifiedCandidate<T>> getRelevantClassifications(final Cluster<C, ? super T> cluster,
            final @NonNull Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex) {
        return cluster.getElements().stream()
                .flatMap(record -> relevantClassificationIndex.getOrDefault(record, List.of()).stream()
                        .filter(classifiedCandidate -> cluster
                                .contains(classifiedCandidate.getCandidate().getRecord2())))
                .collect(Collectors.toList());
    }

    public Stream<Cluster<C, T>> refine(final Stream<? extends Cluster<C, T>> clusters,
            final @NonNull Stream<ClassifiedCandidate<T>> knownClassifications) {
        final Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex =
                this.getRelevantClassificationIndex(knownClassifications);
        return clusters.flatMap(cluster -> this.refineCluster(cluster,
                this.getRelevantClassifications(cluster, relevantClassificationIndex)));
    }

    private Map<T, List<ClassifiedCandidate<T>>> getRelevantClassificationIndex(
            final Stream<ClassifiedCandidate<T>> knownClassifications) {
        return knownClassifications
                .collect(Collectors.groupingBy(classification -> classification.getCandidate().getRecord1()));
    }

    private byte[] refineBigCluster(final @NonNull Cluster<C, T> cluster,
            final @NonNull Collection<ClassifiedCandidate<T>> knownClassifications) {
        final List<WeightedEdge> duplicates = this.toWeightedEdges(knownClassifications, cluster);
        final int desiredNumEdges = gaussianSum(this.maxSmallClusterSize);

        final GreedyClustering<C, T> greedyClustering = new GreedyClustering<>();
        return greedyClustering.greedyCluster(cluster, this.getWeightedEdges(cluster, duplicates, desiredNumEdges));
    }

    /**
     * Performs perfect clustering by maximizing intra-cluster similarity and minimizing inter-cluster similarity.<br>
     * Quite compute-heavy for larger clusters as we perform
     * <li>a complete pair-wise comparison (expensive and quadratic)</li>
     * <li>and compare EACH possible clustering (cheap and exponential).</li>
     *
     * @return the best clustering
     */
    private byte[] refineSmallCluster(final @NonNull Cluster<C, T> cluster,
            final @NonNull Iterable<ClassifiedCandidate<T>> knownClassifications) {
        final double[][] weightMatrix = this.getKnownWeightMatrix(cluster, knownClassifications);

        final int n = cluster.size();
        for (int rowIndex = 0; rowIndex < n; rowIndex++) {
            for (int colIndex = rowIndex + 1; colIndex < n; colIndex++) {
                if (Double.isNaN(weightMatrix[rowIndex][colIndex])) {
                    weightMatrix[rowIndex][colIndex] =
                            getWeight(this.classifier
                                    .classify(new OnlineCandidate<>(cluster.get(rowIndex), cluster.get(colIndex))));
                }
            }
        }

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ClusteringGenerator((byte) n), 0), false)
                .map(clustering -> new AbstractMap.SimpleEntry<>(clustering.clone(),
                        scoreClustering(clustering, weightMatrix)))
                .max(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("Non-empty clusters should have one valid clustering"));
    }

    private List<WeightedEdge> toWeightedEdges(final Collection<ClassifiedCandidate<T>> knownClassifications,
            final Cluster<C, ? extends T> cluster) {
        final Map<T, Integer> clusterIndex =
                IntStream.range(0, cluster.size()).boxed().collect(Collectors.toMap(cluster::get, i -> i));

        return knownClassifications.stream()
                .map(knownClassification -> WeightedEdge.of(
                        clusterIndex.get(knownClassification.getCandidate().getRecord1()),
                        clusterIndex.get(knownClassification.getCandidate().getRecord2()),
                        getWeight(knownClassification.getClassificationResult())))
                .collect(Collectors.toList());
    }

    private Stream<Cluster<C, T>> refineCluster(final Cluster<C, T> cluster,
            final @NonNull Collection<ClassifiedCandidate<T>> knownClassifications) {
        if (cluster.size() <= 2) {
            return Stream.of(cluster);
        }

        final byte[] bestClustering = this.getBestClustering(cluster, knownClassifications);
        return this.getSubClusters(bestClustering, cluster);
    }

    private byte[] getBestClustering(final Cluster<C, T> cluster,
            final @NonNull Collection<ClassifiedCandidate<T>> knownClassifications) {
        if (cluster.size() > this.maxSmallClusterSize) {
            // large cluster with high probability of error
            return this.refineBigCluster(cluster, knownClassifications);
        }

        return this.refineSmallCluster(cluster, knownClassifications);
    }

    private @NonNull double[][] getKnownWeightMatrix(final Cluster<C, ? extends T> cluster,
            final @NonNull Iterable<ClassifiedCandidate<T>> knownClassifications) {
        final int n = cluster.size();
        final double[][] weightMatrix = new double[n][n];
        for (final double[] row : weightMatrix) {
            Arrays.fill(row, Double.NaN);
        }

        final Map<T, Integer> clusterIndex = IntStream.range(0, n)
                .boxed()
                .collect(Collectors.toMap(cluster::get, i -> i));

        for (final ClassifiedCandidate<T> knownClassification : knownClassifications) {
            final int firstIndex = clusterIndex.get(knownClassification.getCandidate().getRecord1());
            final int secondIndex = clusterIndex.get(knownClassification.getCandidate().getRecord2());
            weightMatrix[Math.min(firstIndex, secondIndex)][Math.max(firstIndex, secondIndex)] =
                    getWeight(knownClassification.getClassificationResult());
        }
        return weightMatrix;
    }

    private Stream<Cluster<C, T>> getSubClusters(final byte[] bestClustering,
            final @NonNull Cluster<C, ? extends T> cluster) {
        final Map<Byte, List<T>> subClusters = IntStream.range(0, bestClustering.length)
                .mapToObj(index -> new AbstractMap.SimpleEntry<>(bestClustering[index], cluster.get(index)))
                .collect(Collectors
                        .groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        return subClusters.values().stream()
                .map(records -> new Cluster<>(this.clusterIdGenerator.apply(records), records));
    }

    private List<WeightedEdge> addRandomEdges(final @NonNull List<? extends WeightedEdge> edges,
            final int desiredNumEdges) {
        final Set<WeightedEdge> weightedEdges = new LinkedHashSet<>(edges);
        for (int distance = 2; distance < this.maxSmallClusterSize && weightedEdges.size() < desiredNumEdges;
                distance++) {
            // add random edges with distance 2..n of known edges (e.g., neighbors of known edges).
            final List<WeightedEdge> lastAddedEdges = edges.stream()
                    .flatMap(edge -> edges.stream().filter(edge::overlaps).map(edge::getTriangleEdge))
                    .filter(edge -> !weightedEdges.contains(edge))
                    .limit((long) desiredNumEdges - edges.size())
                    .collect(Collectors.toList());
            Collections.shuffle(lastAddedEdges);
            weightedEdges.addAll(lastAddedEdges);
        }
        if (weightedEdges.size() < desiredNumEdges) {
            throw new IllegalStateException("We have a connected component, so we should get a fully connected graph");
        }
        return new ArrayList<>(weightedEdges);
    }

    private List<WeightedEdge> getWeightedEdges(final @NonNull Cluster<C, ? extends T> cluster,
            final List<? extends WeightedEdge> duplicates,
            final int desiredNumEdges) {
        final List<WeightedEdge> edges = this.getEdges(cluster, duplicates, desiredNumEdges);

        return edges.stream()
                .map(edge -> this.calculateWeightIfNeeded(cluster, edge))
                .collect(Collectors.toList());
    }

    private List<WeightedEdge> getEdges(final @NonNull Cluster<C, ? extends T> cluster,
            final List<? extends WeightedEdge> duplicates, final int desiredNumEdges) {
        if (duplicates.isEmpty()) {
            final int n = cluster.size();
            return getRandomEdges(gaussianSum(n), desiredNumEdges);
        }

        Collections.shuffle(duplicates);
        return this.addRandomEdges(duplicates, desiredNumEdges);

    }

    private WeightedEdge calculateWeightIfNeeded(final @NonNull Cluster<C, ? extends T> cluster,
            final WeightedEdge weightedEdge) {
        final double weight = weightedEdge.getWeight();
        if (Double.isNaN(weight)) {
            // calculate weight for dummy entry
            final T left = cluster.get(weightedEdge.getLeft());
            final T right = cluster.get(weightedEdge.getRight());
            return weightedEdge.withWeight(getWeight(this.classifier.classify(new OnlineCandidate<>(left, right))));
        }
        return weightedEdge;
    }

    @FieldDefaults(level = AccessLevel.PRIVATE)
    private static final class ClusteringGenerator implements Iterator<byte[]> {
        final byte n;
        final @NonNull byte[] clustering;
        boolean hasNext = true;

        private ClusteringGenerator(final byte n) {
            this.n = n;
            this.clustering = new byte[n];
        }

        @Override
        public boolean hasNext() {
            if (this.hasNext) {
                return true;
            }
            for (byte i = (byte) (this.n - (byte) 1); i > 0; i--) {
                if (this.clustering[i] < this.n && !this.incrementWouldResultInSkippedInteger(i)) {
                    this.clustering[i]++;
                    Arrays.fill(this.clustering, i + 1, this.n, (byte) 0);
                    this.hasNext = true;
                    return true;
                }
            }
            return false;
        }

        @Override
        public @NonNull byte[] next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.hasNext = false;
            return this.clustering;
        }

        private boolean incrementWouldResultInSkippedInteger(final byte i) {
            for (byte j = (byte) (i - 1); j >= 0; j--) {
                if (this.clustering[i] <= this.clustering[j]) {
                    return false;
                }
            }
            return true;
        }
    }

    @Value
    static class WeightedEdge {
        @Getter
        int left;
        @Getter
        int right;
        @Wither
        double weight;

        static WeightedEdge of(final int leftIndex, final int rightIndex, final double weight) {
            return new WeightedEdge(Math.min(leftIndex, rightIndex), Math.max(leftIndex, rightIndex), weight);
        }

        private @NonNull WeightedEdge getTriangleEdge(final @NonNull WeightedEdge e) {
            if (this.left < e.left) {
                return new WeightedEdge(this.left, e.left + e.right - this.right, Double.NaN);
            } else if (this.left == e.left) {
                return new WeightedEdge(Math.min(this.right, e.right), Math.max(this.right, e.right), Double.NaN);
            }
            return new WeightedEdge(e.left, this.left + this.getRight() - e.getRight(), Double.NaN);
        }

        private boolean overlaps(final @NonNull WeightedEdge e) {
            return e.getLeft() == this.getLeft() || e.getLeft() == this.getRight() || e.getRight() == this.getLeft()
                    || e.getRight() == this
                    .getRight();
        }
    }

    static class GreedyClustering<C extends Comparable<C>, T> {

        byte[] greedyCluster(final Cluster<C, T> cluster, final @NonNull Collection<? extends WeightedEdge> edges) {

            final Collection<WeightedEdge> queue = new PriorityQueue<>(Comparator.comparing(WeightedEdge::getWeight));
            queue.addAll(edges);

            final double[][] weightMatrix = new double[cluster.size()][cluster.size()];
            for (final WeightedEdge edge : edges) {
                weightMatrix[edge.getLeft()][edge.getRight()] = edge.getWeight();
            }

            // start with each publication in its own cluster
            byte[] clustering = Bytes.toArray(IntStream.range(0, cluster.size()).boxed().collect(Collectors.toList()));
            double score = scoreClustering(clustering, weightMatrix);
            for (final WeightedEdge edge : queue) {
                final byte[] newClustering = clustering.clone();
                final byte newClusterId = newClustering[edge.getLeft()];
                final byte oldClusterId = newClustering[edge.getRight()];
                for (int i = 0; i < newClustering.length; i++) {
                    if (newClustering[i] == oldClusterId) {
                        newClustering[i] = newClusterId;
                    }
                }
                final double newScore = scoreClustering(newClustering, weightMatrix);
                if (newScore > score) {
                    score = newScore;
                    clustering = newClustering;
                }
            }
            return clustering;
        }
    }
}
