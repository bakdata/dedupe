package com.bakdata.deduplication.fusion;

import com.bakdata.util.FunctionalClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.Wither;

@SuppressWarnings({"WeakerAccess", "unused"})
public class ConflictResolutions {
    private final static ThreadLocalRandom random = ThreadLocalRandom.current();

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

    public static <T> ConflictResolution<T, T> saveAs(ConflictResolution<T, T> resolution, ResolutionTag<T> resolutionTag) {
        return new TaggedResolution<>(resolution, resolutionTag);
    }

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

    public static <T> Merge.MergeBuilder<T> merge(Supplier<T> ctor) {
        return Merge.builder(ctor);
    }

    public static final <T> Merge.MergeBuilder<T> merge(Class<T> clazz) {
        return Merge.builder(clazz);
    }

    public static boolean isNonEmpty(Object value) {
        return value != null && !value.equals("");
    }

    public static <T extends Comparable<T>> ConflictResolution<T, T> min() {
        return ((values, context) -> values.stream().min(comparator())
                .map(min -> values.stream().filter(v -> v.getValue().equals(min.getValue())).collect(Collectors.toList()))
                .orElse(List.of()));
    }

    @Value
    private static class TaggedResolution<T, R> implements ConflictResolution<T, R> {
        private final ConflictResolution<T, R> resolution;
        private final ResolutionTag<R> resolutionTag;

        @Override
        public List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> values, FusionContext context) {
            final List<AnnotatedValue<R>> annotatedValues = resolution.resolvePartially(values, context);
            context.storeValues(resolutionTag, annotatedValues);
            return annotatedValues;
        }
    }

    @Value
    public static class Merge<R> implements ConflictResolution<R, R> {
        private final Supplier<R> ctor;
        private final List<FieldMerge<?, R>> fieldMerges;

        public static <R> MergeBuilder<R> builder(Supplier<R> ctor) {
            return new MergeBuilder<>(ctor, FunctionalClass.from((Class<R>) ctor.get().getClass()));
        }

        public static <R> MergeBuilder<R> builder(Class<R> clazz) {
            FunctionalClass<R> f = FunctionalClass.from(clazz);
            return new MergeBuilder<>(f.getConstructor(), f);
        }

        @Override
        public List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<R>> annotatedValues, FusionContext context) {
            final R r = ctor.get();
            for (FieldMerge<?, R> fieldMerge : fieldMerges) {
                fieldMerge.mergeInto(r, annotatedValues, context);
            }
            return List.of(AnnotatedValue.calculated(r));
        }

        @Value
        private static class FieldMerge<T, R> {
            Function<R, T> getter;
            BiConsumer<R, T> setter;
            @Wither
            ConflictResolution<T, T> resolution;

            void mergeInto(R r, List<AnnotatedValue<R>> annotatedValues, FusionContext context) {
                final List<AnnotatedValue<T>> fieldValues = annotatedValues.stream()
                        .map(ar -> ar.withValue(getter.apply(ar.getValue())))
                        .filter(ar -> isNonEmpty(ar.getValue()))
                        .collect(Collectors.toList());
                context.safeExecute(() -> {
                    final Optional<T> resolvedValue = resolution.resolve(fieldValues, context);
                    resolvedValue.ifPresent(v -> setter.accept(r, v));
                });
            }
        }

        @Value
        public static class MergeBuilder<R> {
            Supplier<R> ctor;
            FunctionalClass<R> clazz;
            List<FieldMerge<?, R>> fieldMerges = new ArrayList<>();

            public <F> FieldMergeBuilder<F, R> field(Function<R, F> getter, BiConsumer<R, F> setter) {
                return new FieldMergeBuilder<>(this, getter, setter);
            }

            public <F> FieldMergeBuilder<F, R> field(FunctionalClass.Field<R, F> field) {
                Function<R, F> getter = field.getGetter();
                BiConsumer<R, F> setter = field.getSetter();
                return field(getter, setter);
            }

            public <F> FieldMergeBuilder<F, R> field(String name) {
                FunctionalClass.Field<R, F> field = clazz.field(name);
                return field(field);
            }

            void replaceLast(FieldMerge<?, R> fieldMerge) {
                this.fieldMerges.set(this.fieldMerges.size() - 1, fieldMerge);
            }

            FieldMerge<?, R> getLast() {
                if (fieldMerges.isEmpty()) {
                    throw new IllegalStateException();
                }
                return fieldMerges.get(fieldMerges.size() - 1);
            }

            public ConflictResolution<R, R> build() {
                return new Merge<>(ctor, fieldMerges);
            }

            private void add(FieldMerge<?, R> fieldMerge) {
                this.fieldMerges.add(fieldMerge);
            }
        }

        @Value
        public static class FieldMergeBuilder<F, R> {
            MergeBuilder<R> mergeBuilder;
            Function<R, F> getter;
            BiConsumer<R, F> setter;

            public AdditionalFieldMergeBuilder<F, R> with(ConflictResolution<F, F> resolution) {
                return new AdditionalFieldMergeBuilder<>(convertingWith(resolution));
            }

            @SafeVarargs
            public final AdditionalFieldMergeBuilder<F, R> with(ConflictResolution<F, F> resolution, ConflictResolution<F, F>... resolutions) {
                return with(Arrays.stream(resolutions).reduce(resolution, ConflictResolution::andThen));
            }

            public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(ConflictResolution<F, I> resolution) {
                return new IllTypedFieldMergeBuilder<>(this, resolution);
            }

            public AdditionalFieldMergeBuilder<F, R> corresponding(ResolutionTag<?> tag) {
                return this.with(ConflictResolutions.corresponding(tag));
            }

            @SuppressWarnings("unchecked")
            public AdditionalFieldMergeBuilder<F, R> correspondingToPrevious() {
                final var last = mergeBuilder.getLast();
                ResolutionTag tag;
                // auto tag previous merge if it is not tagged already
                if (last.getResolution() instanceof TaggedResolution) {
                    tag = ((TaggedResolution<?, ?>) last.getResolution()).getResolutionTag();
                } else {
                    var fieldMerges = mergeBuilder.getFieldMerges();
                    tag = new ResolutionTag<>("tag-" + System.identityHashCode(fieldMerges) + "-" + fieldMerges.size());
                    mergeBuilder.replaceLast(last.withResolution(saveAs(last.getResolution(), tag)));
                }
                return corresponding(tag);
            }

            void finish(ConflictResolution<F, F> resolution) {
                mergeBuilder.add(new FieldMerge<>(getter, setter, resolution));
            }
        }

        @Value
        public static class IllTypedFieldMergeBuilder<I, F, R> {
            FieldMergeBuilder<F, R> fieldMergeBuilder;
            ConflictResolution<F, I> resolution;

            public <J> IllTypedFieldMergeBuilder<J, F, R> then(ConflictResolution<I, J> resolution) {
                return new IllTypedFieldMergeBuilder<>(fieldMergeBuilder, this.resolution.andThen(resolution));
            }

            public AdditionalFieldMergeBuilder<F, R> convertingBack(ConflictResolution<I, F> resolution) {
                return new AdditionalFieldMergeBuilder<>(then(resolution));
            }

            MergeBuilder<R> getMergeBuilder() {
                return getFieldMergeBuilder().getMergeBuilder();
            }
        }

        @Value
        public static class AdditionalFieldMergeBuilder<F, R> {
            IllTypedFieldMergeBuilder<F, F, R> inner;

            public <F2> FieldMergeBuilder<F2, R> field(Function<R, F2> getter, BiConsumer<R, F2> setter) {
                inner.getFieldMergeBuilder().finish(inner.getResolution());
                return new FieldMergeBuilder<>(inner.getMergeBuilder(), getter, setter);
            }

            public <F2> FieldMergeBuilder<F2, R> field(FunctionalClass.Field<R, F2> field) {
                Function<R, F2> getter = field.getGetter();
                BiConsumer<R, F2> setter = field.getSetter();
                return field(getter, setter);
            }

            public <F2> FieldMergeBuilder<F2, R> field(String name) {
                FunctionalClass<R> clazz = inner.getMergeBuilder().getClazz();
                FunctionalClass.Field<R, F2> field = clazz.field(name);
                return field(field);
            }

            public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(ConflictResolution<F, I> resolution) {
                return inner.then(resolution);
            }

            public AdditionalFieldMergeBuilder<F, R> then(ConflictResolution<F, F> resolution) {
                return new AdditionalFieldMergeBuilder<>(inner.then(resolution));
            }

            public ConflictResolution<R, R> build() {
                inner.getFieldMergeBuilder().finish(inner.getResolution());
                return inner.getMergeBuilder().build();
            }
        }
    }
}