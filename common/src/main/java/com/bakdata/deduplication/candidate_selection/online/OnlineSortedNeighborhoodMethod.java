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
package com.bakdata.deduplication.candidate_selection.online;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.candidate_selection.SortingKey;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@Builder
public class OnlineSortedNeighborhoodMethod<T> implements OnlineCandidateSelection<T> {
    @Singular
    List<Pass<T>> passes;
    @Builder.Default
    int defaultWindowSize = 10;

    public List<Candidate<T>> getCandidates(T newRecord) {
        return passes.stream().flatMap(pass -> pass.getCandidates(newRecord).stream()).distinct().collect(Collectors.toList());
    }

    @Value
    public static class Pass<T> {
        SortingKey<T> sortingKey;
        int windowSize;
        TreeMap<Comparable<?>, List<T>> index = new TreeMap<>();

        List<Candidate<T>> getCandidates(T newRecord) {
            final Comparable<?> newKey = sortingKey.getKeyExtractor().apply(newRecord);
            final Stream<T> largerRecords = index.tailMap(newKey).values().stream().flatMap(List::stream).limit(windowSize / 2);
            final Stream<T> smallerRecords = index.descendingMap().tailMap(newKey).values().stream().flatMap(List::stream).limit(windowSize / 2);
            final List<Candidate<T>> candidates = Stream.concat(smallerRecords, largerRecords)
                    .map(oldRecord -> new Candidate<>(newRecord, oldRecord))
                    .collect(Collectors.toList());
            index.computeIfAbsent(newKey, key -> new LinkedList<>()).add(newRecord);
            return candidates;
        }
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public static class OnlineSortedNeighborhoodMethodBuilder<T> {

        public OnlineSortedNeighborhoodMethodBuilder<T> sortingKey(SortingKey<T> sortingKey, int windowSize) {
            return pass(new Pass<>(sortingKey, windowSize));
        }

        public OnlineSortedNeighborhoodMethodBuilder<T> sortingKey(SortingKey<T> sortingKey) {
            return sortingKey(sortingKey, defaultWindowSize);
        }
    }
}
