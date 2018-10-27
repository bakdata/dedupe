package com.bakdata.deduplication.candidate_selection;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

import java.util.List;

@Value
@Builder
public class CompositeValue<T extends Comparable<?>> implements Comparable<CompositeValue<T>> {
    @Singular
    List<T> components;

    public static <T extends Comparable<?>> CompositeValue<T> of(T... values) {
        return new CompositeValue<>(List.of(values));
    }

    @Override
    public int compareTo(CompositeValue<T> o) {
        for (int index = 0; index < components.size(); index++) {
            int result = ((Comparable<Object>) components.get(index)).compareTo(o.components.get(index));
            if(result != 0) {
                return result;
            }
        }
        return 0;
    }
}
