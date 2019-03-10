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
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import com.bakdata.dedupe.similarity.StableMatching.Match;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.junit.jupiter.api.Test;

class StableMatchingTest {
    @Test
    void shouldFindStableMatchesWithRankings() {
        List<Queue<Integer>> mensFavoriteWomen = Arrays.asList(
                new LinkedList<>(Arrays.asList(1, 0, 2, 3)),
                new LinkedList<>(Arrays.asList(3, 0, 1, 2)),
                new LinkedList<>(Arrays.asList(0, 2, 1, 3)),
                new LinkedList<>(Arrays.asList(1, 2, 0, 3))
        );
        List<Map<Integer, Integer>> womensRankingForMen = Arrays.asList(
                Map.of(0, 0, 2, 1, 1, 2, 3, 3),
                Map.of(2, 0, 3, 1, 0, 2, 1, 3),
                Map.of(3, 0, 1, 1, 2, 2, 0, 3),
                Map.of(2, 0, 1, 1, 0, 2, 3, 3)
        );

        final Map<Integer, Integer> matches =
                StableMatching.getStableMatches(mensFavoriteWomen, womensRankingForMen);

        assertThat(matches).containsExactly(entry(0, 0), entry(1, 3), entry(2, 2), entry(3, 1));
    }

    @Test
    void shouldFindStableMatches() {
        List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");
        List<String> women = Arrays.asList("abcd", "cc", "ab", "bb");

        final StableMatching<CharSequence> stableMatching = new StableMatching<>(levenshtein());

        final List<Match<CharSequence>> matches =
                stableMatching.getMatches(men, women, SimilarityContext.builder().build());
        assertThat(matches).usingFieldByFieldElementComparator().containsExactlyInAnyOrder(
                new StableMatching.Match("ccc", "cc", 2 / 3f),
                new StableMatching.Match("bbb", "bb", 2 / 3f),
                new StableMatching.Match("aaa", "ab", 1 / 3f),
                new StableMatching.Match("ddd", "abcd", 1 / 4f)
        );
    }

    @Test
    void shouldFindStableMatchesWithAdditionalMen() {
        List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc", "x");
        List<String> women = Arrays.asList("abcd", "cc", "ab", "bb");

        final StableMatching<CharSequence> stableMatching = new StableMatching<>(levenshtein());

        final List<Match<CharSequence>> matches =
                stableMatching.getMatches(men, women, SimilarityContext.builder().build());
        assertThat(matches).usingFieldByFieldElementComparator().containsExactlyInAnyOrder(
                new StableMatching.Match("ccc", "cc", 2 / 3f),
                new StableMatching.Match("bbb", "bb", 2 / 3f),
                new StableMatching.Match("aaa", "ab", 1 / 3f),
                new StableMatching.Match("ddd", "abcd", 1 / 4f)
        );
    }

    @Test
    void shouldFindStableMatchesWithAdditionalWomen() {
        List<String> men = Arrays.asList("aaa", "ddd", "bbb", "ccc");
        List<String> women = Arrays.asList("abcd", "cc", "ab", "bb", "x");

        final StableMatching<CharSequence> stableMatching = new StableMatching<>(levenshtein());

        final List<Match<CharSequence>> matches =
                stableMatching.getMatches(men, women, SimilarityContext.builder().build());
        assertThat(matches).usingFieldByFieldElementComparator().containsExactlyInAnyOrder(
                new StableMatching.Match("ccc", "cc", 2 / 3f),
                new StableMatching.Match("bbb", "bb", 2 / 3f),
                new StableMatching.Match("aaa", "ab", 1 / 3f),
                new StableMatching.Match("ddd", "abcd", 1 / 4f)
        );
    }

}
