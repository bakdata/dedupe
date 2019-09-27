package com.bakdata.dedupe.clustering;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.bakdata.dedupe.clustering.RefineCluster.createGaussPair;
import static com.bakdata.dedupe.clustering.RefineCluster.getRandomEdges;
import static org.assertj.core.api.Assertions.assertThat;

class RefineClusterTest {

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

}