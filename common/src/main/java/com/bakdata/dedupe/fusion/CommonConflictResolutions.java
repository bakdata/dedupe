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
package com.bakdata.dedupe.fusion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.experimental.UtilityClass;


/**
 * Provides factory methods for common conflict resolutions. Usually, these methods are included through static
 * imports.
 * <p>For new resolutions, please open an issue or PR on <a href="https://github.com/bakdata/dedupe/">Github</a>
 * }.</p>
 */
@UtilityClass
public class CommonConflictResolutions {
    private static final ThreadLocalRandom random = ThreadLocalRandom.current();

    /**
     * Retains all values coming from the same source as the values resolved by the {@link ResolutionTag}.
     *
     * @param resolutionTag the tag of the referenced, resolved value
     * @param <T> the type of the value.
     * @return a conflict resolution selecting values from the corresponding source.
     * @see #saveAs(ConflictResolution, ResolutionTag)
     */
    public static <T> ConflictResolution<T, T> corresponding(final ResolutionTag<?> resolutionTag) {
        return ((values, context) -> {
            final Set<Source> sources = context.retrieveValues(resolutionTag).stream()
                    .map(AnnotatedValue::getSource)
                    .collect(Collectors.toSet());
            return values.stream().filter(v -> sources.contains(v.getSource())).collect(Collectors.toList());
        });
    }

    /**
     * Creates a {@link ResolutionTag} for the currently resolved value.
     * <p>The tag can be used by {@link #corresponding(ResolutionTag)} to select the source of the (partially) resolved
     * value(s).</p>
     *
     * @param resolution the current resolution.
     * @param resolutionTag the resolution tag with which the resolved values are accessible.
     * @param <T> the type of the value.
     * @return a tagged resolution.
     */
    public static <T> ConflictResolution<T, T> saveAs(final ConflictResolution<T, T> resolution,
            final ResolutionTag<T> resolutionTag) {
        return new TaggedResolution<>(resolution, resolutionTag);
    }

    private static <T extends Comparable<? super T>> Comparator<AnnotatedValue<T>> comparator() {
        return Comparator.comparing(AnnotatedValue::getValue);
    }

    /**
     * Selects all values that a maximal.
     * <p>This resolution may select multiple values that are equally maximal. These value retain their
     * source tags. Use a successive {@link #first()} to pick any value with the respective source information or use
     * {@link #distinct()}.</p>
     *
     * @param <T> the type of the value.
     * @return the maximal values.
     */
    public static <T extends Comparable<? super T>> ConflictResolution<T, T> max() {
        return ((values, context) -> values.stream().max(comparator())
                .map(max -> values.stream()
                        .filter(v -> v.getValue().equals(max.getValue()))
                        .collect(Collectors.toList()))
                .orElse(List.of()));
    }

    /**
     * Calculates the mean value of a bag of conflicting numbers.
     *
     * @param <T> the type of the value.
     * @return the mean values.
     */
    public static <T extends Number> TerminalConflictResolution<T, Double> mean() {
        return ((values, context) -> values.stream()
                .mapToDouble(v -> v.getValue().doubleValue()).average()
                // workaround for OptionalDouble not having #map
                .stream().boxed().findFirst()
                .map(AnnotatedValue::calculated));
    }

    /**
     * Calculates the sum of a bag of conflicting numbers.
     *
     * @param <T> the type of the value.
     * @return the sum of the values.
     */
    public static <T extends Number> TerminalConflictResolution<T, Double> sum() {
        return ((values, context) -> values.stream()
                .mapToDouble(v -> v.getValue().doubleValue())
                .reduce((agg, v) -> agg + v)
                // workaround for OptionalDouble not having #map
                .stream().boxed().findFirst()
                .map(AnnotatedValue::calculated));
    }

    /**
     * Picks a random value out of the bag of values. This resolution is mostly used as a tie-breaker to receive a
     * {@link TerminalConflictResolution}.
     *
     * @param <T> the type of the value.
     * @return a random value.
     */
    public static <T extends Number> TerminalConflictResolution<T, T> random() {
        return ((values, context) -> values.isEmpty() ? Optional.empty()
                : Optional.of(values.get(random.nextInt(values.size()))));
    }

    /**
     * Picks a the first value out of the bag of values. This resolution is mostly used as a tie-breaker to receive a
     * {@link TerminalConflictResolution}.
     * <p>Note, {@link ConflictResolution} usually prevail order unless noted otherwise. If order is not prevailed,
     * this method should be conceptional treated as {@link #random()}.</p>
     *
     * @param <T> the type of the value.
     * @return the first value.
     */
    public static <T> TerminalConflictResolution<T, T> first() {
        return ((values, context) -> values.stream().findFirst());
    }

    /**
     * Picks a the last value out of the bag of values. This resolution is mostly used as a tie-breaker to receive a
     * {@link TerminalConflictResolution}.
     * <p>Note, {@link ConflictResolution} usually prevail order unless noted otherwise. If order is not prevailed,
     * this method should be conceptional treated as {@link #random()}.</p>
     *
     * @param <T> the type of the value.
     * @return the last value.
     */
    public static <T> TerminalConflictResolution<T, T> last() {
        return ((values, context) -> values.stream().skip(Math.max(0, values.size() - 1)).findAny());
    }

    /**
     * Returns the list of values that are unique. Order is prevailed on the first occurrence.
     * <p>If a value appears multiple times, the resulting {@link AnnotatedValue} will have a calculated source.</p>
     *
     * @param <T> the type of the value.
     * @return the distinct values.
     * @see AnnotatedValue#calculated(Object)
     */
    public static <T> ConflictResolution<T, T> distinct() {
        return ((values, context) -> List.copyOf(values.stream()
                .collect(Collectors.toMap(AnnotatedValue::getValue,
                        v -> v,
                        mergeValuesAsCalculated(),
                        LinkedHashMap::new))
                .values()));
    }

    private static <T> BinaryOperator<@NonNull AnnotatedValue<T>> mergeValuesAsCalculated() {
        return (v1, v2) -> AnnotatedValue.calculated(v1.getValue());
    }

    /**
     * Calculates the median value of a bag of conflicting numbers. If there are two median values, returns them both.
     * <p>Thus, in many cases using a successive {@link #mean()} will yield the expected terminal resolution.</p>
     *
     * @param <T> the type of the value.
     * @return the median values.
     */
    public static <T extends Comparable<T>> ConflictResolution<T, T> median() {
        return ((values, context) -> {
            final List<AnnotatedValue<T>> sorted = new ArrayList<>(values);
            sorted.sort(comparator());
            // create copy of list of median value(s), such that original list is not referenced anymore
            return List.copyOf(sorted
                    .subList((int) Math.floor(sorted.size() / 2.0), (int) Math.ceil(sorted.size() / 2.0)));
        });
    }

    /**
     * Selects all values with the same shortest length.
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the shortest values.
     */
    public static <T extends CharSequence> ConflictResolution<T, T> shortest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream()
                        .collect(Collectors.groupingBy(v -> v.getValue().length(), TreeMap::new, Collectors.toList()))
                        .firstEntry()
                        .getValue());
    }

    /**
     * Selects all values with the same longest length.
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the longest values.
     */
    public static <T extends CharSequence> ConflictResolution<T, T> longest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream()
                        .collect(Collectors.groupingBy(v -> v.getValue().length(), TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue());
    }

    /**
     * Starts the creation of a {@link Merge} conflict resolution for record types.
     * <pre>Example: {@code
     * ConflictResolution<Person, Person> personMerge = merge(Person::new)
     *   .field(Person::getId, Person::setId).with(min())
     *   .field(Person::getFirstName, Person::setFirstName).with(longest()).then(vote())
     *   .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
     *   .build();
     * }
     * </pre>
     *
     * @param instanceSupplier creates a new instance of the class.
     * @return a new {@link com.bakdata.dedupe.fusion.Merge.MergeBuilder}.
     */
    public static <T> Merge.MergeBuilder<T> merge(final @NonNull Supplier<T> instanceSupplier) {
        return Merge.builder(instanceSupplier);
    }

    /**
     * Starts the creation of a {@link Merge} conflict resolution for record types.
     * <pre>Example: {@code
     * ConflictResolution<Person, Person> personMerge = merge(Person.class)
     *   .field(Person::getId, Person::setId).with(min())
     *   .field(Person::getFirstName, Person::setFirstName).with(longest()).then(vote())
     *   .field(Person::getLastName, Person::setLastName).correspondingToPrevious()
     *   .build();
     * }
     * </pre>
     *
     * @param clazz the type of the record. Needs to have a public, parameterless constructor.
     * @return a new {@link com.bakdata.dedupe.fusion.Merge.MergeBuilder}.
     */
    public static <T> Merge.MergeBuilder<T> merge(final Class<T> clazz) {
        return Merge.builder(clazz);
    }

    /**
     * Selects the most frequent values. The values ["a", "b", "a", "c", "c"] will yield ["a", "c"].
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the most frequent values.
     */
    public static <T> ConflictResolution<T, T> mostFrequent() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(AnnotatedValue::getValue))
                        .entrySet()
                        .stream()
                        .collect(Collectors
                                .groupingBy(entry -> entry.getValue().size(), TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue()
                        .stream()
                        .flatMap(entry -> entry.getValue().stream())
                        .collect(Collectors.toList()));
    }

    /**
     * Selects all values with the same earliest {@link AnnotatedValue#getDateTime()}.
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the earliest values.
     */
    public static <T> ConflictResolution<T, T> earliest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream()
                        .collect(Collectors.groupingBy(AnnotatedValue::getDateTime, TreeMap::new, Collectors.toList()))
                        .firstEntry()
                        .getValue());
    }

    /**
     * Selects all values with the same latest {@link AnnotatedValue#getDateTime()}.
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the latest values.
     */
    public static <T> ConflictResolution<T, T> latest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream()
                        .collect(Collectors.groupingBy(AnnotatedValue::getDateTime, TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue());
    }

    /**
     * Selects the highest weighted values. If a value occurs multiple times, the weight of all {@link Source}s is
     * added. Multiple values from the same source count individually.
     * <p>These value retain their source tags.</p>
     *
     * @param <T> the type of the value.
     * @return the most frequent values.
     */
    public static <T> ConflictResolution<T, T> vote() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(AnnotatedValue::getValue))
                        .entrySet()
                        .stream()
                        .collect(Collectors.groupingBy(
                                entry -> entry.getValue().stream().mapToDouble(v -> v.getSource().getWeight()).sum(),
                                TreeMap::new,
                                Collectors.toList()))
                        .lastEntry()
                        .getValue()
                        .stream()
                        .flatMap(entry -> entry.getValue().stream())
                        .collect(Collectors.toList()));
    }

    /**
     * Chooses all values from a specific source. If no value from the primary source exists, the other values from the
     * second source are returned and so on.
     *
     * @param sourcePriority a list of prioritized Sources.
     * @param <T> the type of the value.
     * @return all values from a specific source.
     */
    public static <T> ConflictResolution<T, T> preferSource(final Source... sourcePriority) {
        return preferSource(List.of(sourcePriority));
    }

    /**
     * Chooses all values from a specific source. If no value from the primary source exists, the other values from the
     * second source are returned and so on.
     *
     * @param sourcePriority a list of prioritized Sources.
     * @param <T> the type of the value.
     * @return all values from a specific source.
     */
    public static <T> ConflictResolution<T, T> preferSource(final @NonNull List<Source> sourcePriority) {
        return ((values, context) -> values.stream()
                .map(AnnotatedValue::getSource)
                .min(Comparator.comparingInt(sourcePriority::indexOf))
                .map(source -> values.stream().filter(v -> v.getSource().equals(source)).collect(Collectors.toList()))
                .orElse(List.of()));
    }

    /**
     * Retains all distinct values.
     *
     * @param <E> the type of the elements.
     * @param <C> the type of the collection.
     * @return all distinct values.
     */
    public static <E, C extends Collection<E>> TerminalConflictResolution<C, Set<E>> union() {
        return unionAll(HashSet::new);
    }

    /**
     * Retains all values.
     *
     * @param <E> the type of the elements.
     * @param <C> the type of the collection.
     * @return all values.
     */
    public static <E, C extends Collection<E>> TerminalConflictResolution<C, List<E>> unionAll() {
        return unionAll(ArrayList::new);
    }

    /**
     * Adds all values to the given collection type. Depending on the type, the values are unique or not.
     *
     * @param <E> the type of the elements.
     * @param <C> the type of the collection.
     * @return all values.
     */
    public static <E, C extends Collection<E>, R extends Collection<E>> TerminalConflictResolution<C, R> unionAll(
            final @NonNull Supplier<? extends R> factory) {
        return (annotatedValues, context) -> Optional.of(AnnotatedValue.calculated(
                annotatedValues.stream()
                        .flatMap(value -> value.getValue().stream())
                        .collect(Collectors.toCollection(factory))));
    }

    /**
     * A no-op conflict resolution that will eventually lead to a {@link FusionException}, when values are not
     * resolved.
     *
     * @param <T> the type of the value.
     * @return the values.
     */
    public static <T> ConflictResolution<T, T> assumeEqualValue() {
        return (annotatedValues, context) -> annotatedValues;
    }

    /**
     * Transforms the values within an {@link AnnotatedValue}. The transformation could be used to normalize or convert
     * values before applying an appropriate conflict resolution.
     *
     * @param transform the transformation to apply on the values.
     * @param <T> the type of the value.
     * @param <R>the type of the transformed value.
     * @return a transformed value.
     */
    public static <T, R> ConflictResolution<T, R> transform(final @NonNull Function<? super T, R> transform) {
        return (annotatedValues, context) -> annotatedValues.stream()
                .map(annotatedValue -> annotatedValue.withValue(transform.apply(annotatedValue.getValue())))
                .collect(Collectors.toList());
    }

    /**
     * Selects all values that a minimal.
     * <p>This resolution may select multiple values that are not equal but are comparable. These value retain their
     * source tags. Use a successive {@link #first()} to pick any value with the respective source information.
     * </p>
     *
     * @param <T> the type of the value.
     * @return the minimal values.
     */
    public static <T extends Comparable<T>> ConflictResolution<T, T> min() {
        return ((values, context) -> values.stream().min(comparator())
                .map(min -> values.stream().filter(v -> v.getValue().equals(min.getValue()))
                        .collect(Collectors.toList()))
                .orElse(List.of()));
    }

}
