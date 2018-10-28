package com.bakdata.util;

import lombok.experimental.UtilityClass;

@UtilityClass
public class ObjectUtils {

    public static boolean isNonEmpty(Object value) {
        return value != null && !value.equals("");
    }
}
