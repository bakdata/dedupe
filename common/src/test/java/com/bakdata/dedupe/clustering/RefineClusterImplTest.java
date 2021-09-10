package com.bakdata.dedupe.clustering;

import static com.bakdata.dedupe.clustering.RefineClusterImpl.createGaussPair;
import static com.bakdata.dedupe.clustering.RefineClusterImpl.getRandomEdges;
import static com.bakdata.dedupe.clustering.RefineClusterImpl.triangularNumber;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.classifier.Classification;
import com.bakdata.dedupe.classifier.ClassificationResult;
import com.bakdata.dedupe.classifier.ClassificationResult.ClassificationResultBuilder;
import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import com.bakdata.dedupe.classifier.Classifier;
import com.bakdata.dedupe.clustering.RefineClusterImpl.WeightedEdge;
import java.util.Arrays;
import java.util.List;
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
        final int potentialNumEdges = 55; // gaussian sum of 11
        final int desiredNumEdges = 45; // gaussian sum of 10
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
        final String id;
        final String name;
    }

    public static class CustomClassifier implements Classifier<Person> {

        private static ClassificationResult evaluate(final @NonNull Candidate<? extends Person> candidate) {
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