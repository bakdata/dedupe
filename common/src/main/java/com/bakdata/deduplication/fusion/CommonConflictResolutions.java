package com.bakdata.deduplication.fusion;

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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Value;

@SuppressWarnings({"WeakerAccess", "unused"})
public class CommonConflictResolutions {
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

    public static <T> ConflictResolution<T, T> corresponding(ResolutionTag<?> resolutionTag) {
        return ((values, context) -> {
            if (values.isEmpty()) {
                return values;
            }
            final Set<Source> sources = context.retrieveValues(resolutionTag).stream()
                    .map(AnnotatedValue::getSource).collect(Collectors.toSet());
            return values.stream().filter(v -> sources.contains(v.getSource())).collect(Collectors.toList());
        });
    }

    public static <T> ConflictResolution<T, T> saveAs(ConflictResolution<T, T> resolution, ResolutionTag<T> resolutionTag) {
        return new TaggedResolution<>(resolution, resolutionTag);
    }

    private static <T extends Comparable<? super T>, A extends AnnotatedValue<T>> Comparator<A> comparator() {
        return Comparator.comparing(AnnotatedValue::getValue);
    }

    public static <T, A extends AnnotatedValue<T>> Comparator<A> comparator(Comparator<T> comparator) {
        return Comparator.comparing(AnnotatedValue::getValue, comparator);
    }

    public static <T extends Comparable<? super T>> ConflictResolution<T, T> max() {
        return ((values, context) -> values.stream().max(comparator())
                .map(max -> values.stream().filter(v -> v.getValue().equals(max.getValue())).collect(Collectors.toList()))
                .orElse(List.of()));
    }

    public static <T extends Number> TerminalConflictResolution<T, Double> mean() {
        return ((values, context) -> values.stream()
                .mapToDouble(v -> v.getValue().doubleValue()).average()
                // workaround for OptionalDouble not having #map
                .stream().boxed().findFirst()
                .map(AnnotatedValue::calculated));
    }

    public static <T extends Number> TerminalConflictResolution<T, Double> sum() {
        return ((values, context) -> values.stream()
                .mapToDouble(v -> v.getValue().doubleValue())
                .reduce((agg, v) -> agg + v)
                // workaround for OptionalDouble not having #map
                .stream().boxed().findFirst()
                .map(AnnotatedValue::calculated));
    }

    public static <T extends Number> TerminalConflictResolution<T, T> random() {
        return ((values, context) -> values.isEmpty() ? Optional.empty() : Optional.of(values.get(random.nextInt(values.size()))));
    }

    public static <T> TerminalConflictResolution<T, T> first() {
        return ((values, context) -> values.stream().findFirst());
    }

    public static <T> TerminalConflictResolution<T, T> last() {
        return ((values, context) -> values.stream().skip(Math.max(0, values.size() - 1)).findAny());
    }

    public static <T> ConflictResolution<T, T> distinct() {
        return ((values, context) -> List.copyOf(values.stream()
                .collect(Collectors.toMap(AnnotatedValue::getValue,
                        v -> v,
                        (v1, v2) -> AnnotatedValue.calculated(v1.getValue()),
                        LinkedHashMap::new))
                .values()));
    }

    public static <T extends Comparable<T>> ConflictResolution<T, T> median() {
        return ((values, context) -> {
            if (values.isEmpty()) {
                return values;
            }
            final ArrayList<AnnotatedValue<T>> sorted = new ArrayList<>(values);
            sorted.sort(comparator());
            // create copy of list of median value(s), such that original list is not referenced anymore
            return List.copyOf(sorted.subList((int) Math.floor(sorted.size() / 2.0), (int) Math.ceil(sorted.size() / 2.0)));
        });
    }

    public static <T extends CharSequence> ConflictResolution<T, T> shortest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(v -> v.getValue().length(), TreeMap::new, Collectors.toList()))
                        .firstEntry()
                        .getValue());
    }

    public static <T extends CharSequence> ConflictResolution<T, T> longest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(v -> v.getValue().length(), TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue());
    }

    public static <T> ConflictResolution<T, T> mostFrequent() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(AnnotatedValue::getValue))
                        .entrySet()
                        .stream()
                        .collect(Collectors.groupingBy(entry -> entry.getValue().size(), TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue()
                        .stream()
                        .flatMap(entry -> entry.getValue().stream())
                        .collect(Collectors.toList()));
    }

    public static <T> ConflictResolution<T, T> earliest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(AnnotatedValue::getDateTime, TreeMap::new, Collectors.toList()))
                        .firstEntry()
                        .getValue());
    }

    public static <T> ConflictResolution<T, T> latest() {
        return ((values, context) -> values.isEmpty() ? values :
                values.stream().collect(Collectors.groupingBy(AnnotatedValue::getDateTime, TreeMap::new, Collectors.toList()))
                        .lastEntry()
                        .getValue());
    }

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

    public static <T> ConflictResolution<T, T> preferSource(Source... sourcePriority) {
        return preferSource(List.of(sourcePriority));
    }

    public static <T> ConflictResolution<T, T> preferSource(List<Source> sourcePriority) {
        return ((values, context) -> values.stream()
                .map(AnnotatedValue::getSource)
                .min(Comparator.comparingInt(sourcePriority::indexOf))
                .map(source -> values.stream().filter(v -> v.getSource().equals(source)).collect(Collectors.toList()))
                .orElse(List.of()));
    }

    public static <E, T extends Collection<E>> TerminalConflictResolution<T, Set<E>> union() {
        return unionAll(HashSet::new);
    }

    public static <E, T extends Collection<E>> TerminalConflictResolution<T, List<E>> unionAll() {
        return unionAll(ArrayList::new);
    }

    public static <E, T extends Collection<E>, R extends Collection<E>> TerminalConflictResolution<T, R> unionAll(Supplier<? extends R> ctor) {
        return (annotatedValues, context) -> {
            final R collection = ctor.get();
            for (AnnotatedValue<T> annotatedValue : annotatedValues) {
                collection.addAll(annotatedValue.getValue());
            }
            return Optional.of(AnnotatedValue.calculated(collection));
        };
    }

    public static <T> ConflictResolution<T, T> assumeEqualValue() {
        return (annotatedValues, context) -> annotatedValues;
    }

    public static <T, R> ConflictResolution<T, R> transform(Function<T, R> transform) {
        return (annotatedValues, context) -> annotatedValues.stream()
                .map(annotatedValue -> annotatedValue.withValue(transform.apply(annotatedValue.getValue())))
                .collect(Collectors.toList());
    }

    public static <T extends Comparable<T>> ConflictResolution<T, T> min() {
        return ((values, context) -> values.stream().min(comparator())
                .map(min -> values.stream().filter(v -> v.getValue().equals(min.getValue())).collect(Collectors.toList()))
                .orElse(List.of()));
    }

    @Value
    static class TaggedResolution<T, R> implements ConflictResolution<T, R> {
        private final ConflictResolution<T, R> resolution;
        private final ResolutionTag<R> resolutionTag;

        @Override
        public List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> values, FusionContext context) {
            final List<AnnotatedValue<R>> annotatedValues = resolution.resolvePartially(values, context);
            context.storeValues(resolutionTag, annotatedValues);
            return annotatedValues;
        }
    }
}