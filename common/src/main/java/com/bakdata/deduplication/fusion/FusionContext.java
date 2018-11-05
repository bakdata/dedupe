package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.ExceptionContext;
import lombok.Value;
import lombok.experimental.Delegate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("WeakerAccess")
@Value
public class FusionContext {
    @Delegate
    ExceptionContext exceptionContext = new ExceptionContext();
    Map<ResolutionTag<?>, List<? extends AnnotatedValue<?>>> storedValues = new HashMap<>();

    public <T> void storeValues(ResolutionTag<T> resolutionTag, List<AnnotatedValue<T>> annotatedValues) {
        this.storedValues.put(resolutionTag, annotatedValues);
    }

    @SuppressWarnings("unchecked")
    public <T> List<AnnotatedValue<T>> retrieveValues(ResolutionTag<T> resolutionTag) {
        return (List<AnnotatedValue<T>>) this.storedValues.computeIfAbsent(resolutionTag,
                k -> { throw new FusionException("Tried to retrieve " + resolutionTag + " without being stored"); });
    }
}
