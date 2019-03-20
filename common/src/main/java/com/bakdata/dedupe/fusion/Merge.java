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
import com.bakdata.util.ObjectUtils;
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
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Wither;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Merge<R> implements ConflictResolution<R, R> {
    private final Supplier<R> ctor;
    private final List<FieldMerge<?, R>> fieldMerges;

    @SuppressWarnings("unchecked")
    static <R> MergeBuilder<R> builder(final Supplier<R> ctor) {
        return new MergeBuilder<>(ctor, FunctionalClass.from((Class<R>) ctor.get().getClass()));
    }

    static <R> MergeBuilder<R> builder(final Class<R> clazz) {
        final FunctionalClass<R> f = FunctionalClass.from(clazz);
        return new MergeBuilder<>(f.getConstructor(), f);
    }

    @Override
    public List<AnnotatedValue<R>> resolvePartially(final List<AnnotatedValue<R>> annotatedValues,
            final FusionContext context) {
        final R r = this.ctor.get();
        for (final FieldMerge<?, R> fieldMerge : this.fieldMerges) {
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

        void mergeInto(final R r, final Collection<AnnotatedValue<R>> annotatedValues,
                final FusionContext context) {
            final List<AnnotatedValue<T>> fieldValues = annotatedValues.stream()
                    .map(ar -> ar.withValue(this.getter.apply(ar.getValue())))
                    .filter(ar -> ObjectUtils.isNonEmpty(ar.getValue()))
                    .collect(Collectors.toList());
            context.safeExecute(() -> {
                final Optional<T> resolvedValue = this.resolution.resolve(fieldValues, context);
                resolvedValue.ifPresent(v -> this.setter.accept(r, v));
            });
        }
    }

    @Value
    public static class MergeBuilder<R> {
        Supplier<R> ctor;
        FunctionalClass<R> clazz;
        List<FieldMerge<?, R>> fieldMerges = new ArrayList<>();

        public <F> FieldMergeBuilder<F, R> field(final Function<R, F> getter, final BiConsumer<R, F> setter) {
            return new FieldMergeBuilder<>(this, getter, setter);
        }

        public <F> FieldMergeBuilder<F, R> field(final FunctionalClass.Field<R, F> field) {
            final Function<R, F> getter = field.getGetter();
            final BiConsumer<R, F> setter = field.getSetter();
            return this.field(getter, setter);
        }

        public <F> FieldMergeBuilder<F, R> field(final String name) {
            final FunctionalClass.Field<R, F> field = this.clazz.field(name);
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

        public ConflictResolution<R, R> build() {
            return new Merge<>(this.ctor, this.fieldMerges);
        }

        private void add(final FieldMerge<?, R> fieldMerge) {
            this.fieldMerges.add(fieldMerge);
        }
    }

    @Value
    public static class FieldMergeBuilder<F, R> {
        MergeBuilder<R> mergeBuilder;
        Function<R, F> getter;
        BiConsumer<R, F> setter;

        public AdditionalFieldMergeBuilder<F, R> with(final ConflictResolution<F, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.convertingWith(resolution));
        }

        @SafeVarargs
        public final AdditionalFieldMergeBuilder<F, R> with(final ConflictResolution<F, F> resolution,
                final ConflictResolution<F, F>... resolutions) {
            return this.with(Arrays.stream(resolutions).reduce(resolution, ConflictResolution::andThen));
        }

        public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(final ConflictResolution<F, I> resolution) {
            return new IllTypedFieldMergeBuilder<>(this, resolution);
        }

        public AdditionalFieldMergeBuilder<F, R> corresponding(final ResolutionTag<?> tag) {
            return this.with(CommonConflictResolutions.corresponding(tag));
        }

        @SuppressWarnings("unchecked")
        public AdditionalFieldMergeBuilder<F, R> correspondingToPrevious() {
            final var last = this.mergeBuilder.getLast();
            final ResolutionTag tag;
            // auto tag previous merge if it is not tagged already
            if (last.getResolution() instanceof CommonConflictResolutions.TaggedResolution) {
                tag = ((CommonConflictResolutions.TaggedResolution<?, ?>) last.getResolution()).getResolutionTag();
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
        FieldMergeBuilder<F, R> fieldMergeBuilder;
        ConflictResolution<F, I> resolution;

        public <J> IllTypedFieldMergeBuilder<J, F, R> then(final ConflictResolution<I, J> resolution) {
            return new IllTypedFieldMergeBuilder<>(this.fieldMergeBuilder, this.resolution.andThen(resolution));
        }

        public AdditionalFieldMergeBuilder<F, R> convertingBack(final ConflictResolution<I, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.then(resolution));
        }

        MergeBuilder<R> getMergeBuilder() {
            return this.getFieldMergeBuilder().getMergeBuilder();
        }
    }

    @Value
    public static class AdditionalFieldMergeBuilder<F, R> {
        IllTypedFieldMergeBuilder<F, F, R> inner;

        public <F2> FieldMergeBuilder<F2, R> field(final Function<R, F2> getter, final BiConsumer<R, F2> setter) {
            this.inner.getFieldMergeBuilder().finish(this.inner.getResolution());
            return new FieldMergeBuilder<>(this.inner.getMergeBuilder(), getter, setter);
        }

        public <F2> FieldMergeBuilder<F2, R> field(final FunctionalClass.Field<R, F2> field) {
            final Function<R, F2> getter = field.getGetter();
            final BiConsumer<R, F2> setter = field.getSetter();
            return this.field(getter, setter);
        }

        public <F2> FieldMergeBuilder<F2, R> field(final String name) {
            final FunctionalClass<R> clazz = this.inner.getMergeBuilder().getClazz();
            final FunctionalClass.Field<R, F2> field = clazz.field(name);
            return this.field(field);
        }

        public <I> IllTypedFieldMergeBuilder<I, F, R> convertingWith(final ConflictResolution<F, I> resolution) {
            return this.inner.then(resolution);
        }

        public AdditionalFieldMergeBuilder<F, R> then(final ConflictResolution<F, F> resolution) {
            return new AdditionalFieldMergeBuilder<>(this.inner.then(resolution));
        }

        public ConflictResolution<R, R> build() {
            this.inner.getFieldMergeBuilder().finish(this.inner.getResolution());
            return this.inner.getMergeBuilder().build();
        }
    }
}
