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

package com.bakdata.dedupe.similarity;

import static com.bakdata.dedupe.similarity.CommonSimilarityMeasures.levenshtein;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import com.bakdata.dedupe.matching.WeaklyStableMarriage;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class MatchingSimilarityTest {
    @Test
    void shouldCalculateSimilarityStableMatches() {
        final List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");
        final List<String> women = Arrays.asList("abcd", "cc", "ab", "bb");

        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(men, women, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(
                (1 / 3d + // aaa/ab
                        2 / 3d + // bbb/bb
                        2 / 3d + // ccc/cc
                        1 / 4d) // ddd/abcd
                        / 4, offset(1e-4d)
        );
    }

    @Test
    void shouldCalculateSimilarityStableMatchesWithAdditionalMen() {
        final List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc", "x");
        final List<String> women = Arrays.asList("abcd", "cc", "ab", "bb");

        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(men, women, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(
                (1 / 3d + // aaa/ab
                        2 / 3d + // bbb/bb
                        2 / 3d + // ccc/cc
                        1 / 4d) // ddd/abcd
                        / 5, offset(1e-4d)
        );
    }

    @Test
    void shouldCalculateSimilarityStableMatchesWithAdditionalWomen() {
        final List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");
        final List<String> women = Arrays.asList("abcd", "cc", "ab", "bb", "x");

        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(men, women, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(
                (1 / 3d + // aaa/ab
                        2 / 3d + // bbb/bb
                        2 / 3d + // ccc/cc
                        1 / 4d) // ddd/abcd
                        / 5, offset(1e-4d)
        );
    }

    @Test
    void shouldHandleLeftNullValue() {
        final List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");

        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(null, men, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(SimilarityMeasure.unknown());
    }

    @Test
    void shouldHandleRightNullValue() {
        final List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");

        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(men, null, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(SimilarityMeasure.unknown());
    }

    @Test
    void shouldHandleTwoNullValue() {
        final SimilarityMeasure<List<String>> stableMatching =
                new MatchingSimilarity<>(new WeaklyStableMarriage<>(), levenshtein());

        final double sim =
                stableMatching.getSimilarity(null, null, SimilarityContext.builder().build());
        assertThat(sim).isEqualTo(SimilarityMeasure.unknown());
    }
}
