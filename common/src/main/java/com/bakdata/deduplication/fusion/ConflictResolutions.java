package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.fusion.CommonConflictResolutions.TaggedResolution;
import com.bakdata.util.FunctionalClass;
import com.bakdata.util.ObjectUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.experimental.Wither;

//TODO move to core without moving CommonConflictResolutions if possible
public class ConflictResolutions {

    public static <T> Merge.MergeBuilder<T> merge(Supplier<T> ctor) {
        return Merge.builder(ctor);
    }

    public static final <T> Merge.MergeBuilder<T> merge(Class<T> clazz) {
        return Merge.builder(clazz);
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
                        .filter(ar -> ObjectUtils.isNonEmpty(ar.getValue()))
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
                return this.with(CommonConflictResolutions.corresponding(tag));
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
                    mergeBuilder.replaceLast(last.withResolution(
                        CommonConflictResolutions.saveAs(last.getResolution(), tag)));
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
