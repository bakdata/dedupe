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
package com.bakdata.dedupe.candidate_selection.online;

import com.bakdata.dedupe.candidate_selection.Candidate;
import com.bakdata.dedupe.candidate_selection.SortingKey;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.FieldDefaults;


/**
 * A sorted neighborhood method (SNM) for online deduplication. Records are sorted in multiple passes by a specific
 * sorting key and all records within a window {@code w} are compared.
 * <p>Multiple passes with different windows increase probability that duplicates are sorted near each other in a
 * specific pass. It's best to use a wide range of record attributes and combine them to {@link
 * com.bakdata.dedupe.candidate_selection.CompositeValue} if not discriminating enough. Ideally, each sorting value
 * appears only once per unique duplicate cluster. Longer sorting keys pose almost no runtime overhead and rather small
 * additional memory. Note that multiple passes result in duplicate candidates that are removed within this algorithm.
 * Thus, the actual amount of candidates is usually smaller than {@code #passes * w}.</p>
 * <h2>Window size:</h2>
 * <p>In an online application, the window is constantly changing, such that each record is compared when first
 * inserted and whenever a new record in the current window is inserted.</p>
 * <p>Therefore, a record is compared more than {@code w} times: On average {@code 2*w-1} times, with a lower bound of
 * {@code w-1} (during initial insertion).</p>
 * <p>For comparison, in an offline SNM, each record is compared w-1 times (ignoring the records at the
 * beginning and in the end of the sort index).</p>
 * <p>Thus, this algorithm still preserves the most reliably properties of offline SNM: A linear amount of comparison
 * to the dataset size and a minimum number of comparisons per record.</p>
 *
 * @param <T> the type of the record.
 */
@Value
@Builder
public class OnlineSortedNeighborhoodMethod<T> implements OnlineCandidateSelection<T> {
    /**
     * The different passes used to select the candidates.
     */
    @NonNull
    @Singular
    List<Pass<T, ?>> passes;
    /**
     * The default window size, when not explicitly given. Defaults to 10 but should always be explicitly set when
     * used.
     */
    @Builder.Default
    int defaultWindowSize = 10;

    public @NonNull Stream<Candidate<T>> selectCandidates(final @NonNull T newRecord) {
        return this.passes.stream().flatMap(pass -> pass.getCandidates(newRecord).stream()).distinct();
    }

    /**
     * Represents a pass over the dataset with a specific sorting key and window size.
     *
     * @param <T> the type of the record.
     * @param <K> the type of the sorting key.
     */
    @FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
    @EqualsAndHashCode(exclude = "index")
    public static class Pass<T, K extends Comparable<K>> {
        /**
         * The sorting key to use in this pass.
         */
        @Getter
        @NonNull SortingKey<? super T, ? extends K> sortingKey;
        /**
         * The window {@code >= 2}.
         */
        @Getter
        int windowSize;
        TreeMap<K, List<T>> index = new TreeMap<>();

        /**
         * Creates a pass with the given sorting key and window size.
         *
         * @param sortingKey the sorting key to use in this pass.
         * @param windowSize the window size {@code >= 2}.
         * @throws IllegalArgumentException if {@code windowSize < 2}.
         */
        public Pass(final @NonNull SortingKey<? super T, ? extends K> sortingKey, final int windowSize) {
            if (windowSize < 2) {
                throw new IllegalArgumentException("Window size is < 2: " + windowSize);
            }
            this.sortingKey = sortingKey;
            this.windowSize = windowSize;
        }

        private List<Candidate<T>> getCandidates(final T newRecord) {
            final K newKey = this.sortingKey.getKeyExtractor().apply(newRecord);
            if (newKey == null) {
                return List.of();
            }
            final Stream<T> largerRecords = this.index.tailMap(newKey).values().stream().flatMap(List::stream).limit(
                    this.windowSize / 2);
            final Stream<T> smallerRecords =
                    this.index.descendingMap().tailMap(newKey).values().stream().flatMap(List::stream).limit(
                            this.windowSize / 2);
            final List<Candidate<T>> candidates = Stream.concat(smallerRecords, largerRecords)
                    .map(oldRecord -> new OnlineCandidate<>(newRecord, oldRecord))
                    .collect(Collectors.toList());
            this.index.computeIfAbsent(newKey, key -> new LinkedList<>()).add(newRecord);
            return candidates;
        }
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class OnlineSortedNeighborhoodMethodBuilder<T> {
        /**
         * Adds a new pass with the given sorting key.
         *
         * @param sortingKey the sorting key to use in this pass.
         * @param windowSize the window size {@code >= 2}.
         * @return this
         */
        public OnlineSortedNeighborhoodMethodBuilder<T> sortingKey(final SortingKey<T, ?> sortingKey,
                final int windowSize) {
            return this.pass(new Pass<>(sortingKey, windowSize));
        }

        /**
         * Adds a new pass with the given sorting key and the {@link #defaultWindowSize(int)}.
         *
         * @param sortingKey the sorting key to use in this pass.
         * @return this
         */
        public OnlineSortedNeighborhoodMethodBuilder<T> sortingKey(final SortingKey<T, ?> sortingKey) {
            return this.sortingKey(sortingKey, this.defaultWindowSize);
        }

        /**
         * Adds new passes with the given list of sorting keys and the {@link #defaultWindowSize(int)}.
         *
         * @param sortingKeys the sorting keys to use in these passes.
         * @return this
         */
        public @NonNull OnlineSortedNeighborhoodMethodBuilder<T> sortingKeys(
                final @NonNull Iterable<SortingKey<T, ?>> sortingKeys) {
            return this.sortingKeys(sortingKeys, this.defaultWindowSize);
        }

        /**
         * Adds new passes with the given list of sorting keys and the given window size.
         *
         * @param sortingKeys the sorting keys to use in these passes.
         * @param windowSize the window size {@code >= 2}.
         * @return this
         */
        public @NonNull OnlineSortedNeighborhoodMethodBuilder<T> sortingKeys(
                final @NonNull Iterable<SortingKey<T, ?>> sortingKeys,
                final int windowSize) {
            for (final SortingKey<T, ?> sortingKey : sortingKeys) {
                this.sortingKey(sortingKey, windowSize);
            }
            return this;
        }
    }
}
