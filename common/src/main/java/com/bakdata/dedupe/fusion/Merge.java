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

import com.bakdata.util.FunctionalClass;
import com.bakdata.util.FunctionalProperty;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;


/**
 * A nested conflict resolution for complex types.
 * <p>The individual elements are resolved with specific {@link ConflictResolution} functions and then a new instance
 * of the record type is created with the respective values.</p>
 * <p>If any exception during fusion occurs, the following field resolutions are still applied and a joint {@link
 * FusionException} is thrown.</p>
 *
 * @param <R> the type of the record.
 * @see CommonConflictResolutions#merge(Class)
 * @see CommonConflictResolutions#merge(Supplier)
 */
@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Merge<R> implements ConflictResolution<R, R> {
    @NonNull
    private final Supplier<R> ctor;
    @NonNull
    private final List<FieldMerge<?, R>> fieldMerges;

    @SuppressWarnings("unchecked")
    static <R> MergeBuilder<R> builder(final @NonNull Supplier<R> ctor) {
        return new MergeBuilder<>(ctor, FunctionalClass.of((Class<R>) ctor.get().getClass()));
    }

    static <R> MergeBuilder<R> builder(final Class<R> clazz) {
        final FunctionalClass<R> f = FunctionalClass.of(clazz);
        return new MergeBuilder<>(f.getConstructor(), f);
    }

    @Override
    public @NonNull List<@NonNull AnnotatedValue<R>> resolveNonEmptyPartially(
            final @NonNull List<@NonNull AnnotatedValue<R>> annotatedValues, final @NonNull FusionContext context) {
        final R r = this.ctor.get();
        for (final FieldMerge<?, R> fieldMerge : this.fieldMerges) {
            fieldMerge.mergeInto(r, annotatedValues, context);
        }
        return List.of(AnnotatedValue.calculated(r));
    }

    @Value
    private static class FieldMerge<T, R> {
        @NonNull Function<R, T> getter;
        @NonNull BiConsumer<R, T> setter;
        @NonNull
        @Wither
        ConflictResolution<T, T> resolution;

        void mergeInto(final R r, final @NonNull Collection<AnnotatedValue<R>> annotatedValues,
                final @NonNull FusionContext context) {
            final List<AnnotatedValue<T>> fieldValues = annotatedValues.stream()
                    .map(ar -> ar.withValue(this.getter.apply(ar.getValue())))
                    .filter(ar -> context.isNonEmpty(ar.getValue()))
                    .collect(Collectors.toList());
            context.safeExecute(() -> {
                final Optional<T> resolvedValue = this.resolution.resolve(fieldValues, context);
                resolvedValue.ifPresent(v -> this.setter.accept(r, v));
            });
        }
    }

    @Value
    public static class MergeBuilder<R> {
        @NonNull Supplier<R> ctor;
        @NonNull FunctionalClass<R> clazz;
        List<FieldMerge<?, R>> fieldMerges = new ArrayList<>();

        @NonNull
        public <F> FieldMergeBuilder<F, R> field(final Function<R, F> getter, final BiConsumer<R, F> setter) {
            return new FieldMergeBuilder<>(this, getter, setter);
        }

        @NonNull
        public <F> FieldMergeBuilder<F, R> field(final @NonNull FunctionalProperty<R, F> field) {
            final Function<R, F> getter = field.getGetter();
            final BiConsumer<R, F> setter = field.getSetter();
            return this.field(getter, setter);
        }

        @NonNull
        public <F> FieldMergeBuilder<F, R> field(final @NonNull String name) {
            final FunctionalProperty<R, F> field = this.clazz.field(name);
            return this.field(field);
        }

        void replaceLast(final FieldMerge<?, R> fieldMerge) {
            this.fieldMerges.set(this.fieldMerges.size() - 1, fieldMerge);
        }

        @SuppressWarnings("squid:S1452")
        FieldMerge<?, R> getLast() {
            if (this.fieldMerges.isEmpty()) {
                throw new IllegalStateException();
            }
            return this.fieldMerges.get(this.fieldMerges.size() - 1);
        }

        @NonNull
        public ConflictResolution<R, R> build() {
            return new Merge<>(this.ctor, this.fieldMerges);
        }

        private void add(final FieldMerge<?, R> fieldMerge) {
            this.fieldMerges.add(fieldMerge);
        }
    }

    @Value
    public static class FieldMergeBuilder<F, R> {
        @NonNull MergeBuilder<R> mergeBuilder;
        @NonNull Function<R, F> getter;
        @NonNull BiConsumer<R, F> setter;

        @NonNull
        public AdditionalFieldMergeBuilder<F, R> with(final ConflictResolution<F, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.convertingWith(resolution));
        }

        @NonNull
        @SafeVarargs
        public final AdditionalFieldMergeBuilder<F, R> with(final ConflictResolution<F, F> resolution,
                final @NonNull ConflictResolution<F, F>... resolutions) {
            return this.with(Arrays.stream(resolutions).reduce(resolution, ConflictResolution::andThen));
        }

        @NonNull
        public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(final ConflictResolution<F, I> resolution) {
            return new IllTypedFieldMergeBuilder<>(this, resolution);
        }

        @NonNull
        public AdditionalFieldMergeBuilder<F, R> corresponding(final ResolutionTag<?> tag) {
            return this.with(CommonConflictResolutions.corresponding(tag));
        }

        @NonNull
        @SuppressWarnings("unchecked")
        public AdditionalFieldMergeBuilder<F, R> correspondingToPrevious() {
            final var last = this.mergeBuilder.getLast();
            final ResolutionTag tag;
            // auto tag previous merge if it is not tagged already
            if (last.getResolution() instanceof TaggedResolution) {
                tag = ((TaggedResolution<?, ?>) last.getResolution()).getResolutionTag();
            } else {
                final var fieldMerges = this.mergeBuilder.getFieldMerges();
                tag = new ResolutionTag<>("tag-" + System.identityHashCode(fieldMerges) + "-" + fieldMerges.size());
                this.mergeBuilder.replaceLast(last.withResolution(
                        CommonConflictResolutions.saveAs(last.getResolution(), tag)));
            }
            return this.corresponding(tag);
        }

        void finish(final ConflictResolution<F, F> resolution) {
            this.mergeBuilder.add(new FieldMerge<>(this.getter, this.setter, resolution));
        }
    }

    @Value
    public static class IllTypedFieldMergeBuilder<I, F, R> {
        @NonNull FieldMergeBuilder<F, R> fieldMergeBuilder;
        @NonNull ConflictResolution<F, I> resolution;

        @NonNull
        public <J> IllTypedFieldMergeBuilder<J, F, R> then(final ConflictResolution<I, J> resolution) {
            return new IllTypedFieldMergeBuilder<>(this.fieldMergeBuilder, this.resolution.andThen(resolution));
        }

        @NonNull
        public AdditionalFieldMergeBuilder<F, R> convertingBack(final ConflictResolution<I, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.then(resolution));
        }

        MergeBuilder<R> getMergeBuilder() {
            return this.getFieldMergeBuilder().getMergeBuilder();
        }
    }

    @Value
    public static class AdditionalFieldMergeBuilder<F, R> {
        @NonNull IllTypedFieldMergeBuilder<F, F, R> inner;

        @NonNull
        public <F2> FieldMergeBuilder<F2, R> field(final Function<R, F2> getter, final BiConsumer<R, F2> setter) {
            this.inner.getFieldMergeBuilder().finish(this.inner.getResolution());
            return new FieldMergeBuilder<>(this.inner.getMergeBuilder(), getter, setter);
        }

        @NonNull
        public <F2> FieldMergeBuilder<F2, R> field(final @NonNull FunctionalProperty<R, F2> field) {
            final Function<R, F2> getter = field.getGetter();
            final BiConsumer<R, F2> setter = field.getSetter();
            return this.field(getter, setter);
        }

        @NonNull
        public <F2> FieldMergeBuilder<F2, R> field(final @NonNull String name) {
            final FunctionalClass<R> clazz = this.inner.getMergeBuilder().getClazz();
            final FunctionalProperty<R, F2> field = clazz.field(name);
            return this.field(field);
        }

        @NonNull
        public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(final ConflictResolution<F, I> resolution) {
            return this.inner.then(resolution);
        }

        @NonNull
        public AdditionalFieldMergeBuilder<F, R> then(final ConflictResolution<F, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.inner.then(resolution));
        }

        @NonNull
        public ConflictResolution<R, R> build() {
            this.inner.getFieldMergeBuilder().finish(this.inner.getResolution());
            return this.inner.getMergeBuilder().build();
        }
    }
}
