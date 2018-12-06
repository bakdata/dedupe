/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.bakdata.deduplication.classifier.Classifier;
import com.google.common.primitives.Bytes;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Wither;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Value
@Builder
public class RefineCluster<C extends Comparable<C>, T> {
    private static final int MAX_SUB_CLUSTERS = 100;
    @Builder.Default
    int maxSmallClusterSize = 10;
    @NonNull
    Classifier<T> classifier;
    @NonNull
    Function<Iterable<T>, C> clusterIdGenerator;

    private static float getWeight(Classification classification) {
        switch (classification.getResult()) {
            case DUPLICATE:
                return classification.getConfidence();
            case NON_DUPLICATE:
                return -classification.getConfidence();
            case UNKNOWN:
                return -0f;
            default:
                throw new IllegalStateException();
        }
    }

    public List<Cluster<C, T>> refine(List<Cluster<C, T>> transitiveClosure, Iterable<ClassifiedCandidate<T>> knownClassifications) {
        final Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex = getRelevantClassificationIndex(knownClassifications);
        return transitiveClosure.stream()
                .flatMap(cluster -> refineCluster(cluster, getRelevantClassifications(cluster, relevantClassificationIndex)))
                .collect(Collectors.toList());
    }

    private List<ClassifiedCandidate<T>> getRelevantClassifications(Cluster<C, T> cluster, Map<T, List<ClassifiedCandidate<T>>> relevantClassificationIndex) {
        return cluster.getElements().stream()
                .flatMap(record -> relevantClassificationIndex.getOrDefault(record, List.of()).stream()
                        .filter(classifiedCandidate -> cluster.contains(classifiedCandidate.getCandidate().getOldRecord())))
                .collect(Collectors.toList());
    }

    private Map<T, List<ClassifiedCandidate<T>>> getRelevantClassificationIndex(Iterable<ClassifiedCandidate<T>> knownClassifications) {
        Map<T, List<ClassifiedCandidate<T>>> relevantClassifications = new HashMap<>();
        for (ClassifiedCandidate<T> knownClassification : knownClassifications) {
            final Candidate<T> candidate = knownClassification.getCandidate();
            relevantClassifications.computeIfAbsent(candidate.getNewRecord(), r -> new LinkedList<>()).add(knownClassification);
        }
        return relevantClassifications;
    }

    private byte[] refineBigCluster(Cluster<C, T> cluster, List<ClassifiedCandidate<T>> knownClassifications) {
        final List<WeightedEdge> duplicates = toWeightedEdges(knownClassifications, cluster);
        final int desiredNumEdges = getNumEdges(maxSmallClusterSize);

        return greedyCluster(cluster, getWeightedEdges(cluster, duplicates, desiredNumEdges));
    }

    /**
     * Performs perfect clustering by maximizing intra-cluster similarity and minimizing inter-cluster similarity.<br>
     * Quite compute-heavy for larger clusters as we perform
     * <li>a complete pair-wise comparison (expensive and quadratic)</li>
     * <li>and compare EACH possible clustering (cheap and exponential).</li>
     *
     * @return the best clustering
     */
    private byte[] refineSmallCluster(Cluster<C, T> cluster, List<ClassifiedCandidate<T>> knownClassifications) {
        float[][] weightMatrix = getKnownWeightMatrix(cluster, knownClassifications);

        final int n = cluster.size();
        for (int rowIndex = 0; rowIndex < n; rowIndex++) {
            for (int colIndex = rowIndex + 1; colIndex < n; colIndex++) {
                if (Float.isNaN(weightMatrix[rowIndex][colIndex])) {
                    weightMatrix[rowIndex][colIndex] =
                            getWeight(classifier.classify(new Candidate<>(cluster.get(rowIndex), cluster.get(colIndex))));
                }
            }
        }

        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ClusteringGenerator((byte) n), 0), false)
                .map(clustering -> new AbstractMap.SimpleEntry<>(clustering.clone(), scoreClustering(clustering, weightMatrix)))
                .max(Comparator.comparingDouble(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("Non-empty clusters should have one valid clustering"));
    }

    private List<WeightedEdge> toWeightedEdges(List<ClassifiedCandidate<T>> knownClassifications, Cluster<C, T> cluster) {
        final Map<T, Integer> clusterIndex =
                IntStream.range(0, cluster.size()).boxed().collect(Collectors.toMap(cluster::get, i -> i));

        return knownClassifications.stream()
                .map(knownClassification ->
                        WeightedEdge.of(clusterIndex.get(knownClassification.getCandidate().getNewRecord()),
                                clusterIndex.get(knownClassification.getCandidate().getOldRecord()),
                                getWeight(knownClassification.getClassification())))
                .collect(Collectors.toList());
    }

    private float[][] getKnownWeightMatrix(Cluster<C, T> cluster, List<ClassifiedCandidate<T>> knownClassifications) {
        var n = cluster.size();
        var weightMatrix = new float[n][n];
        for (var row : weightMatrix) {
            Arrays.fill(row, Float.NaN);
        }

        var clusterIndex =
                IntStream.range(0, n).boxed().collect(Collectors.toMap(cluster::get, i -> i));
        for (ClassifiedCandidate<T> knownClassification : knownClassifications) {
            var firstIndex = clusterIndex.get(knownClassification.getCandidate().getNewRecord());
            var secondIndex = clusterIndex.get(knownClassification.getCandidate().getOldRecord());
            weightMatrix[Math.min(firstIndex, secondIndex)][Math.max(firstIndex, secondIndex)] =
                    getWeight(knownClassification.getClassification());
        }
        return weightMatrix;
    }

    private Stream<Cluster<C, T>> refineCluster(Cluster<C, T> cluster, List<ClassifiedCandidate<T>> knownClassifications) {
        if (cluster.size() <= 2) {
            return Stream.of(cluster);
        }

        byte[] bestClustering;
        if (cluster.size() > maxSmallClusterSize) {
            // large cluster with high probability of error
            bestClustering = refineBigCluster(cluster, knownClassifications);

        } else {
            bestClustering = refineSmallCluster(cluster, knownClassifications);
        }

        return getSubClusters(bestClustering, cluster);
    }

    private Stream<Cluster<C, T>> getSubClusters(byte[] bestClustering, Cluster<C, T> cluster) {
        final Map<Byte, List<T>> subClusters = IntStream.range(0, bestClustering.length)
                .mapToObj(index -> new AbstractMap.SimpleEntry<>(bestClustering[index], cluster.get(index)))
                .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));
        return subClusters.values().stream()
                .map(records -> new Cluster<>(clusterIdGenerator.apply(records), records));
    }

    private byte[] greedyCluster(Cluster<C, T> cluster, List<WeightedEdge> edges) {

        final PriorityQueue<WeightedEdge> queue = new PriorityQueue<>(Comparator.comparing(WeightedEdge::getWeight));
        queue.addAll(edges);

        float[][] weightMatrix = new float[cluster.size()][cluster.size()];
        for (WeightedEdge edge : edges) {
            weightMatrix[edge.left][edge.right] = edge.weight;
        }

        // start with each publication in its own cluster
        byte[] clustering = Bytes.toArray(IntStream.range(0, cluster.size()).boxed().collect(Collectors.toList()));
        float score = 0;
        for (WeightedEdge edge : queue) {
            final byte[] newClustering = clustering.clone();
            final byte newClusterId = newClustering[edge.left];
            final byte oldClusterId = newClustering[edge.right];
            for (int i = 0; i < newClustering.length; i++) {
                if (newClustering[i] == oldClusterId) {
                    newClustering[i] = newClusterId;
                }
            }
            final float newScore = scoreClustering(newClustering, weightMatrix);
            if (newScore > score) {
                score = newScore;
                clustering = newClustering;
            }
        }
        return clustering;
    }


    private List<WeightedEdge> getRandomEdges(int potentialNumEdges, int desiredNumEdges) {
        List<WeightedEdge> weightedEdges;
        weightedEdges = new Random().ints(0, potentialNumEdges)
                .distinct()
                .limit(desiredNumEdges)
                .mapToObj(i -> {
                    // reverse of Gaussian
                    int leftIndex = (int) (Math.sqrt(i + .25) - .5);
                    int rightIndex = i - getNumEdges(leftIndex) + leftIndex;
                    return WeightedEdge.of(leftIndex, rightIndex, Float.NaN);
                })
                .collect(Collectors.toList());
        return weightedEdges;
    }

    private int getNumEdges(int n) {
        return n * (n - 1) / 2;
    }

    private List<WeightedEdge> addRandomEdges(List<WeightedEdge> edges, int desiredNumEdges) {
        // add random edges with distance 2..n of known edges (e.g., neighbors of known edges).
        List<WeightedEdge> lastAddedEdges;
        Set<WeightedEdge> weightedEdges = new LinkedHashSet<>(edges);
        for (int distance = 2; distance < maxSmallClusterSize && weightedEdges.size() < desiredNumEdges; distance++) {
            lastAddedEdges = edges.stream()
                    .flatMap(e1 -> edges.stream().filter(e1::overlaps).map(e1::getTriangleEdge))
                    .filter(e -> !weightedEdges.contains(e))
                    .limit((long) desiredNumEdges - edges.size())
                    .collect(Collectors.toList());
            weightedEdges.addAll(lastAddedEdges);
            Collections.shuffle(lastAddedEdges);
        }
        if (weightedEdges.size() < desiredNumEdges) {
            throw new IllegalStateException("We have a connected components, so we should get a fully connected graph");
        }
        return new ArrayList<>(weightedEdges);
    }

    private List<WeightedEdge> getWeightedEdges(Cluster<C, T> cluster, List<WeightedEdge> duplicates, int desiredNumEdges) {
        final List<WeightedEdge> weightedEdges;
        if (duplicates.isEmpty()) {
            final int n = cluster.size();
            weightedEdges = getRandomEdges(getNumEdges(n), desiredNumEdges);
        } else {
            Collections.shuffle(duplicates);
            weightedEdges = addRandomEdges(duplicates, desiredNumEdges);
        }

        return weightedEdges.stream().map(weightedEdge -> {
            float weight = weightedEdge.getWeight();
            if (Float.isNaN(weight)) {
                // calculate weight for dummy entry
                final T left = cluster.get(weightedEdge.getLeft());
                final T right = cluster.get(weightedEdge.getRight());
                return weightedEdge.withWeight(getWeight(classifier.classify(new Candidate<>(left, right))));
            }
            return weightedEdge;
        }).collect(Collectors.toList());
    }

    private float scoreClustering(byte[] partitions, float[][] weightMatrix) {
        final int n = partitions.length;
        int[] partitionSizes = new int[n];
        for (byte clustering : partitions) {
            partitionSizes[clustering]++;
        }

        float score = 0;
        for (int rowIndex = 0; rowIndex < n; rowIndex++) {
            for (int colIndex = rowIndex + 1; colIndex < n; colIndex++) {
                if (partitions[rowIndex] == partitions[colIndex]) {
                    score += weightMatrix[rowIndex][colIndex] / partitionSizes[partitions[rowIndex]];
                } else {
                    score -= weightMatrix[rowIndex][colIndex] / (n - partitionSizes[partitions[rowIndex]]) +
                            weightMatrix[rowIndex][colIndex] / (n - partitionSizes[partitions[colIndex]]);
                }
            }
        }
        return score;
    }

    private static final class ClusteringGenerator implements Iterator<byte[]> {
        final byte n;
        final byte[] clustering;
        boolean hasNext = true;

        ClusteringGenerator(byte n) {
            this.n = n;
            clustering = new byte[n];
        }

        @Override
        public boolean hasNext() {
            if (hasNext) {
                return true;
            }
            for (byte i = (byte) (n - (byte) 1); i > 0; i--) {
                if (clustering[i] < n && !incrementWouldResultInSkippedInteger(i)) {
                    clustering[i]++;
                    Arrays.fill(clustering, i + 1, n, (byte) 0);
                    hasNext = true;
                    return hasNext;
                }
            }
            return false;
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            hasNext = false;
            return clustering;
        }

        private boolean incrementWouldResultInSkippedInteger(byte i) {
            for (byte j = (byte) (i - 1); j >= 0; j--) {
                if (clustering[i] <= clustering[j]) {
                    return false;
                }
            }
            return true;
        }
    }

    @Value
    private static class WeightedEdge {
        int left;
        int right;
        @Wither
        float weight;

        static WeightedEdge of(int leftIndex, int rightIndex, float weight) {
            return new WeightedEdge(Math.min(leftIndex, rightIndex), Math.max(leftIndex, rightIndex), weight);
        }

        WeightedEdge getTriangleEdge(WeightedEdge e) {
            if (left < e.left) {
                return new WeightedEdge(left, e.left + e.right - right, Float.NaN);
            } else if (left == e.left) {
                return new WeightedEdge(Math.min(right, e.right), Math.max(right, e.right), Float.NaN);
            }
            return new WeightedEdge(e.left, left + right - e.right, Float.NaN);
        }

        boolean overlaps(WeightedEdge e) {
            return e.left == left || e.left == right || e.right == left || e.right == right;
        }
    }

}