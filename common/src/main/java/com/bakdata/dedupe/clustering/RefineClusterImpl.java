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
import com.google.common.annotations.VisibleForTesting;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import lombok.experimental.FieldDefaults;
import org.apache.commons.lang3.tuple.Pair;


/**
 * Splits large clusters into smaller clusters when the inter-cluster similarities are suboptimal.
 * <p>{@link RefineClusterImpl} can be applied after aggressive clustering variations (leaning towards recall), such as
 * {@link TransitiveClosure}, to boost the overall precision.</p>
 * <p>To determine the inner-cluster similarities the confidence score of a {@link Classifier} is used. Negative scores
 * are allowed and strongly discourage certain pairs.</p>
 * <p>If all scores within a cluster are positive, the cluster will remain intact.</p>
 * <p>Note, this algorithm uses three strategies depending on the cluster size:</p>
 * <ul>
 *     <li>Clusters up to {@link #maxExhaustiveClusterSize} are classified completely and solved perfectly by
 *     enumerating all possible clusterings. The number of clusterings grows with the Bell number of the cluster size,
 *     which is why this size is limited to {@value #MAX_EXHAUSTIVE_CLUSTER_SIZE}.</li>
 *     <li>Clusters up to {@link #maxSmallClusterSize} are classified completely and solved heuristically by a greedy
 *     clustering followed by a local search with {@link #localSearchRestarts} random restarts.</li>
 *     <li>Larger clusters are only classified partially and solved by a greedy clustering.</li>
 * </ul>
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 * @param <I> the type of the record id.
 */
@Value
@Builder
public class RefineClusterImpl<C extends Comparable<C>, T, I> implements RefineCluster<C, T, I> {
    /**
     * The maximum allowed value for {@link #maxExhaustiveClusterSize}. Bell(12) is already ~4 million clusterings.
     */
    public static final int MAX_EXHAUSTIVE_CLUSTER_SIZE = 12;
    private static final Random RANDOM = ThreadLocalRandom.current();
    private static final long LOCAL_SEARCH_SEED = 0x5DEECE66DL;
    private static final double EPSILON = 1.0e-12;

    /**
     * The maximum size (inclusive) of a cluster for which all pairs are classified. This size limits the maximum
     * amount of comparisons to {@code max * (max - 1) / 2}.
     */
    @Builder.Default
    int maxSmallClusterSize = 10;
    /**
     * The maximum size (inclusive) of a cluster that is solved perfectly by enumerating all possible clusterings. Must
     * not exceed {@value #MAX_EXHAUSTIVE_CLUSTER_SIZE}, because the number of clusterings grows super-exponentially.
     * Only relevant up to {@link #maxSmallClusterSize}.
     */
    @Builder.Default
    int maxExhaustiveClusterSize = 10;
    /**
     * The number of additional random restarts of the local search for clusters larger than
     * {@link #maxExhaustiveClusterSize} and up to {@link #maxSmallClusterSize}.
     */
    @Builder.Default
    int localSearchRestarts = 5;
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
    Function<Iterable<I>, C> clusterIdGenerator;
    /**
     * A function to extract the id of a record.
     */
    @NonNull
    Function<T, I> idExtractor;

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

    @VisibleForTesting
    static double scoreClustering(final int[] partitions, final double[][] weightMatrix) {
        final int n = partitions.length;
        final int[] partitionSizes = new int[n];
        for (final int clustering : partitions) {
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
                .mapToObj(RefineClusterImpl::createGaussPair)
                .filter(RefineClusterImpl::isNotSelfPair)
                .map(p -> WeightedEdge.of(p.getLeft(), p.getRight(), Double.NaN))
                .limit(desiredNumEdges)
                .collect(Collectors.toList());
    }

    private static <T> boolean isNotSelfPair(final Pair<T, T> pair) {
        return !pair.getLeft().equals(pair.getRight());
    }

    @VisibleForTesting
    static Pair<Integer, Integer> createGaussPair(final int i) {
        // reverse of Gaussian
        final int leftIndex = (int) (Math.sqrt(2 * i + 0.25) - 0.5);
        final int rightIndex = i - triangularNumber(leftIndex);
        return Pair.of(leftIndex, rightIndex);
    }

    @VisibleForTesting
    static int triangularNumber(final int n) {
        return (n + 1) * (n) / 2;
    }

    private List<ClassifiedCandidate<T>> getRelevantClassifications(final Cluster<C, ? super T> cluster,
            final @NonNull Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex) {
        return cluster.getElements().stream()
                .flatMap(element -> relevantClassificationIndex.getOrDefault(element, List.of()).stream()
                        .filter(classifiedCandidate -> cluster
                                .contains(classifiedCandidate.getCandidate().getRecord2())))
                .collect(Collectors.toList());
    }

    /**
     * Performs perfect clustering by maximizing intra-cluster similarity and minimizing inter-cluster similarity.<br>
     * Compares EACH possible clustering, i.e., Bell(n) clusterings with O(n²) each. Only feasible for very small n.
     *
     * @return the best clustering
     */
    static int[] exhaustiveClustering(final double[][] weightMatrix) {
        final Iterator<int[]> clusterings = new ClusteringGenerator(weightMatrix.length);
        int[] bestClustering = null;
        double bestScore = Double.NEGATIVE_INFINITY;
        while (clusterings.hasNext()) {
            final int[] clustering = clusterings.next();
            final double score = scoreClustering(clustering, weightMatrix);
            if (bestClustering == null || score > bestScore) {
                bestScore = score;
                bestClustering = clustering.clone();
            }
        }
        if (bestClustering == null) {
            throw new IllegalStateException("Non-empty clusters should have one valid clustering");
        }
        return bestClustering;
    }

    private Map<T, List<ClassifiedCandidate<T>>> getRelevantClassificationIndex(
            final Stream<ClassifiedCandidate<T>> knownClassifications) {
        return knownClassifications
                .collect(Collectors.groupingBy(classification -> classification.getCandidate().getRecord1()));
    }

    private int[] refineBigCluster(final @NonNull Cluster<C, T> cluster,
            final @NonNull Collection<ClassifiedCandidate<T>> knownClassifications) {
        final List<WeightedEdge> duplicates = this.toWeightedEdges(knownClassifications, cluster);
        final int desiredNumEdges = triangularNumber(this.maxSmallClusterSize);

        final GreedyClustering<C, T> greedyClustering = new GreedyClustering<>();
        return greedyClustering.greedyCluster(cluster, this.getWeightedEdges(cluster, duplicates, desiredNumEdges));
    }

    /**
     * Approximates {@link #exhaustiveClustering(double[][])} in polynomial time. Starts with a greedy clustering and
     * additional random clusterings, improves each with a local search, and returns the best result. The random
     * clusterings are seeded deterministically, so the same weights always yield the same clustering.
     *
     * @return the best clustering found
     */
    static int[] heuristicClustering(final double[][] weightMatrix, final int restarts) {
        final int n = weightMatrix.length;
        int[] bestClustering = localSearch(GreedyClustering.greedyCluster(weightMatrix, getEdges(weightMatrix)),
                weightMatrix);
        double bestScore = scoreClustering(bestClustering, weightMatrix);
        final Random random = new Random(LOCAL_SEARCH_SEED);
        final int numInitialClusters = Math.max(2, n / 3);
        for (int restart = 0; restart < restarts; restart++) {
            final int[] start = random.ints(n, 0, numInitialClusters).toArray();
            final int[] clustering = localSearch(start, weightMatrix);
            final double score = scoreClustering(clustering, weightMatrix);
            if (score > bestScore + EPSILON) {
                bestScore = score;
                bestClustering = clustering;
            }
        }
        return bestClustering;
    }

    /**
     * Repeatedly moves single records to the cluster (or a new singleton cluster) that improves the score most, until
     * no move improves the score anymore. Terminates because the score strictly increases with every move.
     *
     * @return a locally optimal clustering
     */
    static int[] localSearch(final int[] start, final double[][] weightMatrix) {
        final int n = weightMatrix.length;
        final int[] clustering = start.clone();
        double score = scoreClustering(clustering, weightMatrix);
        boolean improved = true;
        while (improved) {
            improved = false;
            for (int i = 0; i < n; i++) {
                final int originalCluster = clustering[i];
                int bestCluster = originalCluster;
                for (final int candidateCluster : getMoveTargets(clustering)) {
                    if (candidateCluster == originalCluster) {
                        continue;
                    }
                    clustering[i] = candidateCluster;
                    final double newScore = scoreClustering(clustering, weightMatrix);
                    if (newScore > score + EPSILON) {
                        score = newScore;
                        bestCluster = candidateCluster;
                    }
                }
                clustering[i] = bestCluster;
                improved = improved || bestCluster != originalCluster;
            }
        }
        return clustering;
    }

    /**
     * @return all cluster ids in use plus one unused id, which represents a new singleton cluster.
     */
    private static int[] getMoveTargets(final int[] clustering) {
        final int n = clustering.length;
        final boolean[] used = new boolean[n];
        for (final int clusterId : clustering) {
            used[clusterId] = true;
        }
    	final int newClusterId = IntStream.range(0, n)
            .filter(clusterId -> !used[clusterId])
            .findFirst()
            .orElse(-1);
    	return IntStream.range(0, n)
            .filter(clusterId -> used[clusterId] || clusterId == newClusterId)
            .toArray();

    }

    private static List<WeightedEdge> getEdges(final double[][] weightMatrix) {
        final int n = weightMatrix.length;
        final List<WeightedEdge> edges = new ArrayList<>(triangularNumber(n - 1));
        for (int rowIndex = 0; rowIndex < n; rowIndex++) {
            for (int colIndex = rowIndex + 1; colIndex < n; colIndex++) {
                edges.add(WeightedEdge.of(rowIndex, colIndex, weightMatrix[rowIndex][colIndex]));
            }
        }
        return edges;
    }

    public Stream<Cluster<C, T>> refine(final Stream<? extends Cluster<C, T>> clusters,
            final @NonNull Stream<ClassifiedCandidate<T>> knownClassifications) {
        if (this.maxExhaustiveClusterSize > MAX_EXHAUSTIVE_CLUSTER_SIZE) {
            throw new IllegalArgumentException(String.format(
                    "maxExhaustiveClusterSize must not exceed %d but was %d. Use maxSmallClusterSize to classify "
                            + "larger clusters completely.", MAX_EXHAUSTIVE_CLUSTER_SIZE,
                    this.maxExhaustiveClusterSize));
        }
        if (this.localSearchRestarts < 0) {
            throw new IllegalArgumentException("localSearchRestarts must not be negative but was "
                    + this.localSearchRestarts);
        }
        final Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex =
                this.getRelevantClassificationIndex(knownClassifications);
        return clusters.flatMap(cluster -> this.refineCluster(cluster,
                this.getRelevantClassifications(cluster, relevantClassificationIndex)));
    }

    /**
     * Classifies all pairs of the cluster that are not yet known (quadratic).
     */
    private double[][] getFullWeightMatrix(final @NonNull Cluster<C, ? extends T> cluster,
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
        return weightMatrix;
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

        final int[] bestClustering = this.getBestClustering(cluster, knownClassifications);
        return this.getSubClusters(bestClustering, cluster);
    }

    private int[] getBestClustering(final Cluster<C, T> cluster,
            final @NonNull Collection<ClassifiedCandidate<T>> knownClassifications) {
        if (cluster.size() > this.maxSmallClusterSize) {
            // large cluster with high probability of error
            return this.refineBigCluster(cluster, knownClassifications);
        }

        final double[][] weightMatrix = this.getFullWeightMatrix(cluster, knownClassifications);
        if (cluster.size() <= this.maxExhaustiveClusterSize) {
            return exhaustiveClustering(weightMatrix);
        }
        return heuristicClustering(weightMatrix, this.localSearchRestarts);
    }

    private double[][] getKnownWeightMatrix(final Cluster<C, ? extends T> cluster,
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

    private Stream<Cluster<C, T>> getSubClusters(final int[] bestClustering,
            final @NonNull Cluster<C, ? extends T> cluster) {
        final Map<Integer, List<T>> subClusters = IntStream.range(0, bestClustering.length)
                .mapToObj(index -> new AbstractMap.SimpleEntry<>(bestClustering[index], cluster.get(index)))
                .collect(Collectors
                        .groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        return subClusters.values().stream()
                .map(records -> new Cluster<>(this.clusterIdGenerator.apply(this.getElementIds(records)), records));
    }

    private List<I> getElementIds(final Collection<? extends T> records) {
        return records.stream()
                .map(this.idExtractor)
                .collect(Collectors.toList());
    }

    private List<WeightedEdge> addRandomEdges(final @NonNull List<WeightedEdge> edges,
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
            final List<WeightedEdge> duplicates,
            final int desiredNumEdges) {
        final List<WeightedEdge> edges = this.selectRandomEdges(cluster, duplicates, desiredNumEdges);

        return edges.stream()
                .map(edge -> this.calculateWeightIfNeeded(cluster, edge))
                .collect(Collectors.toList());
    }

    private List<WeightedEdge> selectRandomEdges(final @NonNull Cluster<C, ? extends T> cluster,
            final List<WeightedEdge> duplicates, final int desiredNumEdges) {
        if (duplicates.isEmpty()) {
            final int n = cluster.size();
            return getRandomEdges(triangularNumber(n), desiredNumEdges);
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
    private static final class ClusteringGenerator implements Iterator<int[]> {
        final int n;
        final int[] clustering;
        boolean hasNext = true;

        private ClusteringGenerator(final int n) {
            this.n = n;
            this.clustering = new int[n];
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
        public int[] next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.hasNext = false;
            return this.clustering;
        }

        private boolean incrementWouldResultInSkippedInteger(final int i) {
            for (int j = i - 1; j >= 0; j--) {
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
        @With
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

        /**
         * Merges the clusters of the edges with the highest weights first, as long as the merge improves the score.
         * Ties are broken by the order of the given edges.
         */
        private static int[] greedyCluster(final double[][] weightMatrix,
                final @NonNull Collection<WeightedEdge> edges) {
            final List<WeightedEdge> sortedEdges = edges.stream()
                    .sorted(Comparator.comparingDouble(WeightedEdge::getWeight).reversed())
                    .collect(Collectors.toList());

            // start with each publication in its own cluster
            int[] clustering = IntStream.range(0, weightMatrix.length).toArray();
            double score = scoreClustering(clustering, weightMatrix);
            for (final WeightedEdge edge : sortedEdges) {
                final int[] newClustering = clustering.clone();
                final int newClusterId = newClustering[edge.getLeft()];
                final int oldClusterId = newClustering[edge.getRight()];
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

        int[] greedyCluster(final Cluster<C, T> cluster, final @NonNull Collection<WeightedEdge> edges) {
            final double[][] weightMatrix = new double[cluster.size()][cluster.size()];
            for (final WeightedEdge edge : edges) {
                weightMatrix[edge.getLeft()][edge.getRight()] = edge.getWeight();
            }
            return greedyCluster(weightMatrix, edges);
        }
    }
}
