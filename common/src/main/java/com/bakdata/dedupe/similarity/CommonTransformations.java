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

package com.bakdata.dedupe.similarity;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.ColognePhonetic;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.codec.language.bm.BeiderMorseEncoder;


/**
 * A utility class that offers factory methods for common similarity transformations. Usually, these methods are
 * included through static imports.
 * <p>For new transformations, please open an issue or PR on <a href="https://github.com/bakdata/dedupe/">Github</a>
 * }.</p>
 *
 * @see CommonSimilarityMeasures
 */
@UtilityClass
public class CommonTransformations {
    private static final Splitter WHITE_SPACE_SPLITTER = Splitter.on(Pattern.compile("\\s+"));

    /**
     * Creates a tokenizer for a string into bigrams; that is, two succeeding characters.
     *
     * @param <T> the input type.
     * @return a tokenizer for bigrams.
     */
    public static <T extends CharSequence> ValueTransformation<T, List<CharSequence>> bigram() {
        return ngram(2);
    }

    /**
     * Creates a normalizer that turns strings into the cologne phonetics. This normalizer is suitable for similar
     * sounding German names.
     *
     * @return a normalizer for cologne phonetics.
     * @see <a href="https://en.wikipedia.org/wiki/Cologne_phonetics">cologne phonetics on wikipedia</a>
     * @see ColognePhonetic implementation for details
     */
    public static ValueTransformation<String, String> colognePhonetic() {
        return codec(new ColognePhonetic());
    }

    /**
     * Creates a tokenizer for a string into ngrams; that is, two or more succeeding characters.
     *
     * @param n the number of characters per ngram.
     * @param <T> the input type.
     * @return a tokenizer for bigrams.
     */
    public static <T extends CharSequence> ValueTransformation<T, List<CharSequence>> ngram(final int n) {
        return (t, context) -> IntStream.range(0, t.length() - n + 1)
                .mapToObj(i -> t.subSequence(i, i + n))
                .collect(Collectors.toList());
    }

    /**
     * Creates a normalizer that turns strings into the soundex representation. This normalizer is suitable for similar
     * sounding, English names.
     *
     * @return a normalizer for soundex.
     * @see Soundex implementation for details
     */
    public static ValueTransformation<String, String> soundex() {
        return codec(new Soundex());
    }

    /**
     * Creates a normalizer that turns strings into the refined soundex representation. This normalizer is suitable for
     * similar sounding, English names.
     *
     * @return a normalizer for refined soundex.
     * @see RefinedSoundex implementation for details
     */
    public static ValueTransformation<String, String> refinedSoundex(final @NonNull char[] mapping) {
        return codec(new RefinedSoundex(mapping));
    }

    /**
     * Creates a normalizer that turns strings into the beider morse code. This normalizer is suitable for similar
     * sounding European names.
     *
     * @return a normalizer for beider morse code.
     * @see BeiderMorseEncoder implementation for details
     */
    public static ValueTransformation<String, String> beiderMorse() {
        return codec(new BeiderMorseEncoder());
    }

    /**
     * Wraps a {@link StringEncoder} into a {@link ValueTransformation}.
     *
     * @param encoder the encoder to wrap.
     * @return the wrapped encode.
     * @throws Exception (sneaky)
     */
    public static ValueTransformation<String, String> codec(final @NonNull StringEncoder encoder) {
        return new ValueTransformation<>() {
            @Override
            @SneakyThrows
            public String transform(final @NonNull String s, final @NonNull SimilarityContext context) {
                return encoder.encode(s);
            }
        };
    }

    /**
     * Splits a string on all whitespace characters into words.
     *
     * @param <T> the input type.
     * @return a tokenizer for words.
     */
    public static <T extends CharSequence> ValueTransformation<T, List<String>> words() {
        return (t, context) -> Lists.newArrayList(WHITE_SPACE_SPLITTER.split(t));
    }

    /**
     * Wraps any function into a {@link ValueTransformation}. Most methods that use {@link ValueTransformation}s usually
     * also accepts {@link Function}s, but turning a function explicitly into a {@code ValueTransformation} can be
     * useful for composition.
     *
     * @param function the function to wrap.
     * @param <T> the type of the input.
     * @param <R> the return type of the function.
     * @return the wrapped function.
     */
    public static <T, R> ValueTransformation<T, R> transform(final @NonNull Function<? super T, ? extends R> function) {
        return (t, context) -> function.apply(t);
    }

    /**
     * Creates a tokenizer for a string into trigrams; that is, three succeeding characters.
     *
     * @param <T> the input type.
     * @return a tokenizer for trigrams.
     */
    public static <T extends CharSequence> ValueTransformation<T, List<CharSequence>> trigram() {
        return ngram(3);
    }

}
