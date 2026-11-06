package com.bakdata.dedupe.clustering;

import static com.bakdata.dedupe.clustering.RefineClusterImpl.createGaussPair;
import static com.bakdata.dedupe.clustering.RefineClusterImpl.getRandomEdges;
import static com.bakdata.dedupe.clustering.RefineClusterImpl.scoreClustering;
import static com.bakdata.dedupe.clustering.RefineClusterImpl.triangularNumber;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassificationResult;
import com.bakdata.dedupe.classifier.ClassificationResult.ClassificationResultBuilder;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.bakdata.dedupe.classifier.Classifier;
import com.bakdata.dedupe.clustering.RefineClusterImpl.WeightedEdge;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RefineClusterImplTest {

    static Stream<Arguments> generateTriangularNumbers() {
        return Stream.of(
                Arguments.of(1, 1),
                Arguments.of(2, 3),
                Arguments.of(5, 15),
                Arguments.of(100, 5050)
        );
    }

    @ParameterizedTest
    @MethodSource("generateTriangularNumbers")
    void shouldCalculateTriangularNumber(final int i, final int expected) {
        assertThat(triangularNumber(i))
                .as("%d should return", i, expected)
                .isEqualTo(expected);
    }

    static Stream<Arguments> generateGaussPairs() {
        final int n = 7;
        final AtomicInteger i = new AtomicInteger();
        return IntStream.range(0, n)
                .boxed()
                .flatMap(leftIndex -> IntStream.rangeClosed(0, leftIndex)
                        .boxed()
                        .map(rightIndex -> Pair.of(leftIndex, rightIndex)))
                .map(p -> Arguments.of(i.getAndIncrement(), p.getLeft(), p.getRight()));
    }

    @ParameterizedTest
    @MethodSource("generateGaussPairs")
    void shouldCreateCorrectGaussPair(final int i, final int leftIndex, final int rightIndex) {
        assertThat(createGaussPair(i))
                .as("%d should generate %d and %d", i, leftIndex, rightIndex)
                .satisfies(edge -> assertThat(edge.getLeft()).isEqualTo(leftIndex))
                .satisfies(edge -> assertThat(edge.getRight()).isEqualTo(rightIndex));
    }

    @Test
    void shouldCreateCorrectNumberOfRandomEdges() {
        final int potentialNumEdges = 55; // Gaussian sum of 11
        final int desiredNumEdges = 45; // Gaussian sum of 10
        assertThat(getRandomEdges(potentialNumEdges, desiredNumEdges))
                .hasSize(desiredNumEdges);
    }

    @Test
    void shouldCusterHeuristically() {
        final AtomicLong atomicLong = new AtomicLong();
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> atomicLong.getAndIncrement())
                .maxSmallClusterSize(4)
                .idExtractor(Person::getId)
                .build();

        final Cluster<Long, Person> personCluster = new Cluster<>(0L, List.of(
                new Person("1", "Joanna"),
                new Person("2", "Joanna"),
                new Person("3", "Joanna"),
                new Person("4", "Joanna"),
                new Person("5", "Joanna")
        ));
        final Stream<Cluster<Long, Person>> actual = refineCluster.refine(Stream.of(personCluster), Stream.empty());
        assertThat(actual)
                .hasSize(1)
                .allSatisfy(cluster -> assertThat(cluster).isEqualTo(personCluster));
    }

    @Test
    void shouldCluster() {
        final AtomicLong atomicLong = new AtomicLong();
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> atomicLong.getAndIncrement())
                .maxSmallClusterSize(10)
                .idExtractor(Person::getId)
                .build();

        final Cluster<Long, Person> personCluster = new Cluster<>(0L, List.of(
                new Person("1", "Joanna"),
                new Person("2", "Joanna"),
                new Person("3", "Joanna"),
                new Person("4", "Joanna"),
                new Person("5", "Joanna")
        ));
        final Stream<Cluster<Long, Person>> actual = refineCluster.refine(Stream.of(personCluster), Stream.empty());
        assertThat(actual)
                .hasSize(1)
                .allSatisfy(cluster -> assertThat(cluster).isEqualTo(personCluster));
    }

    @Test
    void shouldRefineCluster() {
        final AtomicLong atomicLong = new AtomicLong();
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> atomicLong.getAndIncrement())
                .maxSmallClusterSize(10)
                .idExtractor(Person::getId)
                .build();

        final Cluster<Long, Person> personCluster = new Cluster<>(0L, List.of(
                new Person("1", "Joanna"),
                new Person("2", "Joanna"),
                new Person("3", "Johanna"),
                new Person("4", "Johanna"),
                new Person("5", "Johanna")
        ));
        final Stream<Cluster<Long, Person>> actual = refineCluster.refine(Stream.of(personCluster), Stream.empty());
        assertThat(actual)
                .hasSize(2)
                .containsExactlyInAnyOrder(
                        new Cluster<>(0L, List.of(
                                new Person("1", "Joanna"),
                                new Person("2", "Joanna"))),
                        new Cluster<>(1L, List.of(
                                new Person("3", "Johanna"),
                                new Person("4", "Johanna"),
                                new Person("5", "Johanna"))));
    }

    @Test
    void shouldRefineClusterHeuristically() {
        final AtomicLong atomicLong = new AtomicLong();
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> atomicLong.getAndIncrement())
                .maxSmallClusterSize(4)
                .idExtractor(Person::getId)
                .build();

        final Cluster<Long, Person> personCluster = new Cluster<>(0L, List.of(
                new Person("1", "Joanna"),
                new Person("2", "Joanna"),
                new Person("3", "Johanna"),
                new Person("4", "Johanna"),
                new Person("5", "Johanna")
        ));
        final Stream<Cluster<Long, Person>> actual = refineCluster.refine(Stream.of(personCluster), Stream.empty());
        assertThat(actual)
                .hasSize(2)
                .containsExactlyInAnyOrder(
                        new Cluster<>(0L, List.of(
                                new Person("1", "Joanna"),
                                new Person("2", "Joanna"))),
                        new Cluster<>(1L, List.of(
                                new Person("3", "Johanna"),
                                new Person("4", "Johanna"),
                                new Person("5", "Johanna"))));
    }

    @Test
    void shouldDoGreedyClustering() {

        final RefineClusterImpl.GreedyClustering<Long, Integer> greedyClustering = new RefineClusterImpl.GreedyClustering<>();

        final Cluster<Long, Integer> cluster = new Cluster<>(1L, List.of(1, 2, 3, 4, 5));
        // Note: Greedy clustering is sensitive to the order, in which edges are added.
        // Changing the order may lead to different results.
        final int[] ints = greedyClustering.greedyCluster(cluster, List.of(
                WeightedEdge.of(0, 1, 1.0),
                WeightedEdge.of(2, 3, 1.0),
                WeightedEdge.of(3, 4, 1.0),
                WeightedEdge.of(1, 3, 1.0)
        ));

        final List<Integer> actual = Arrays.stream(ints).boxed().collect(Collectors.toList());
        assertThat(actual).isEqualTo(List.of(0, 0, 0, 0, 0));
    }

    @Test
    void shouldSplitInGreedyClustering() {

        final RefineClusterImpl.GreedyClustering<Long, Integer> greedyClustering = new RefineClusterImpl.GreedyClustering<>();

        final Cluster<Long, Integer> cluster = new Cluster<>(1L, List.of(1, 2, 3, 4, 5));
        final int[] ints = greedyClustering.greedyCluster(cluster, List.of(
                WeightedEdge.of(0, 1, 1.0),
                WeightedEdge.of(2, 4, 1.0),
                WeightedEdge.of(1, 3, 1.0)
        ));

        final List<Integer> actual = Arrays.stream(ints).boxed().collect(Collectors.toList());
        assertThat(actual).isEqualTo(List.of(0, 0, 2, 0, 2));
    }

    private static double[][] createRandomWeightMatrix(final Random random, final int n) {
        // two to three planted clusters with noisy, partially unknown classifications
        final int numClusters = 2 + random.nextInt(2);
        final int[] labels = random.ints(n, 0, numClusters).toArray();
        final double[][] weightMatrix = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                final boolean duplicate = (labels[i] == labels[j]) == (random.nextDouble() < 0.85);
                final double confidence = 0.3 + 0.7 * random.nextDouble();
                weightMatrix[i][j] = random.nextDouble() < 0.1 ? -0.0 : (duplicate ? confidence : -confidence);
            }
        }
        return weightMatrix;
    }

    @Test
    void shouldMergeHighestWeightsFirstInGreedyClustering() {
        final RefineClusterImpl.GreedyClustering<Long, Integer> greedyClustering =
                new RefineClusterImpl.GreedyClustering<>();

        final Cluster<Long, Integer> cluster = new Cluster<>(1L, List.of(1, 2, 3, 4, 5));
        final int[] ints = greedyClustering.greedyCluster(cluster, List.of(
                WeightedEdge.of(0, 2, 0.5),
                WeightedEdge.of(1, 2, 1.0),
                WeightedEdge.of(1, 3, 0.5),
                WeightedEdge.of(2, 3, -1.0),
                WeightedEdge.of(3, 4, 0.5)
        ));

        final List<Integer> actual = Arrays.stream(ints).boxed().collect(Collectors.toList());
        assertThat(actual).isEqualTo(List.of(0, 0, 0, 3, 3));
    }

    @Test
    void shouldApproximateExhaustiveClustering() {
        final Random random = new Random(42);
        final int numTrials = 200;
        int numOptimal = 0;
        for (int trial = 0; trial < numTrials; trial++) {
            final double[][] weightMatrix = createRandomWeightMatrix(random, 6 + random.nextInt(5));
            final double exhaustiveScore =
                    scoreClustering(RefineClusterImpl.exhaustiveClustering(weightMatrix), weightMatrix);
            final double heuristicScore =
                    scoreClustering(RefineClusterImpl.heuristicClustering(weightMatrix, 5), weightMatrix);
            assertThat(heuristicScore).isLessThanOrEqualTo(exhaustiveScore + 1.0e-9);
            if (heuristicScore >= exhaustiveScore - 1.0e-9) {
                numOptimal++;
            }
        }
        assertThat(numOptimal).isGreaterThanOrEqualTo(numTrials * 9 / 10);
    }

    @Test
    void shouldFindLocalOptimum() {
        final Random random = new Random(7);
        final double[][] weightMatrix = createRandomWeightMatrix(random, 8);
        final int[] clustering = RefineClusterImpl.localSearch(new int[8], weightMatrix);
        final double score = scoreClustering(clustering, weightMatrix);
        for (int i = 0; i < clustering.length; i++) {
            for (int clusterId = 0; clusterId < clustering.length; clusterId++) {
                final int[] moved = clustering.clone();
                moved[i] = clusterId;
                assertThat(scoreClustering(moved, weightMatrix)).isLessThanOrEqualTo(score + 1.0e-9);
            }
        }
    }

    @Test
    void shouldRefineMediumClusterQuickly() {
        final AtomicLong atomicLong = new AtomicLong();
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> atomicLong.getAndIncrement())
                .maxSmallClusterSize(20)
                .idExtractor(Person::getId)
                .build();

        final List<Person> joannas = IntStream.range(0, 12)
                .mapToObj(i -> new Person("a" + i, "Joanna"))
                .collect(Collectors.toList());
        final List<Person> johannas = IntStream.range(0, 8)
                .mapToObj(i -> new Person("b" + i, "Johanna"))
                .collect(Collectors.toList());
        final Cluster<Long, Person> personCluster = new Cluster<>(0L,
                Stream.concat(joannas.stream(), johannas.stream()).collect(Collectors.toList()));

        final List<Cluster<Long, Person>> actual = assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> refineCluster.refine(Stream.of(personCluster), Stream.empty()).collect(Collectors.toList()));
        assertThat(actual)
                .extracting(Cluster::getElements)
                .containsExactlyInAnyOrder(joannas, johannas);
    }

    @Test
    void shouldRejectTooLargeExhaustiveClusterSize() {
        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(personList -> 0L)
                .maxSmallClusterSize(20)
                .maxExhaustiveClusterSize(RefineClusterImpl.MAX_EXHAUSTIVE_CLUSTER_SIZE + 1)
                .idExtractor(Person::getId)
                .build();

        assertThatThrownBy(() -> refineCluster.refine(Stream.empty(), Stream.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxExhaustiveClusterSize");
    }

    @Test
    void shouldRefineClusterWithMoreThan128Elements() {
        final List<Person> firstCluster = IntStream.range(0, 130)
                .boxed()
                .map(i -> new Person(i.toString(), "Joanna"))
                .collect(Collectors.toList());

        final Cluster<Long, Person> cluster = new Cluster<>(1L, firstCluster);

        final RefineClusterImpl<Long, Person, String> refineCluster = RefineClusterImpl.<Long, Person, String>builder()
                .classifier(new CustomClassifier())
                .clusterIdGenerator(list -> 0L)
                .maxSmallClusterSize(120)
                .idExtractor(Person::getId)
                .build();

        final Stream<Cluster<Long, Person>> stream = refineCluster.refine(Stream.of(cluster), Stream.of());
        assertThat(stream).hasSize(1);
    }

    @RequiredArgsConstructor
    @Value
    public static class Person {
        String id;
        String name;
    }

    public static class CustomClassifier implements Classifier<Person> {

        private static ClassificationResult evaluate(final @NonNull Candidate<Person> candidate) {
            final ClassificationResultBuilder confidence = ClassificationResult.builder().confidence(1.0);
            if (candidate.getRecord1().getName().equals(candidate.getRecord2().getName())) {
                return confidence.classification(Classification.DUPLICATE).build();
            }

            return confidence.classification(Classification.NON_DUPLICATE).build();
        }

        @Override
        public @NonNull ClassificationResult classify(final @NonNull Candidate<Person> candidate) {
            return evaluate(candidate);
        }

        @Override
        public @NonNull ClassifiedCandidate<Person> classifyCandidate(
                final @NonNull Candidate<Person> candidate) {
            final ClassificationResult classificationResult = evaluate(candidate);
            return new ClassifiedCandidate<>(candidate, classificationResult);
        }
    }
}
