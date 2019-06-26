/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"count", "text", "line", "word", "character"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Counts various metrics on incoming text. The requested results will be recorded as attributes. "
        + "The resulting flowfile will not have its content modified.")
@WritesAttributes({
        @WritesAttribute(attribute = "text.line.count", description = "The number of lines of text present in the FlowFile content"),
        @WritesAttribute(attribute = "text.line.nonempty.count", description = "The number of lines of text (with at least one non-whitespace character) present in the original FlowFile"),
        @WritesAttribute(attribute = "text.word.count", description = "The number of words present in the original FlowFile"),
        @WritesAttribute(attribute = "text.character.count", description = "The number of characters (given the specified character encoding) present in the original FlowFile"),
})
@SeeAlso(SplitText.class)
public class CountText extends AbstractProcessor {
    private static final List<Charset> STANDARD_CHARSETS = Arrays.asList(
            StandardCharsets.UTF_8,
            StandardCharsets.US_ASCII,
            StandardCharsets.ISO_8859_1,
            StandardCharsets.UTF_16,
            StandardCharsets.UTF_16LE,
            StandardCharsets.UTF_16BE);

    private static final Pattern SYMBOL_PATTERN = Pattern.compile("[\\s-\\._]");
    private static final Pattern WHITESPACE_ONLY_PATTERN = Pattern.compile("\\s");

    // Attribute keys
    public static final String TEXT_LINE_COUNT = "text.line.count";
    public static final String TEXT_LINE_NONEMPTY_COUNT = "text.line.nonempty.count";
    public static final String TEXT_WORD_COUNT = "text.word.count";
    public static final String TEXT_CHARACTER_COUNT = "text.character.count";


    public static final PropertyDescriptor TEXT_LINE_COUNT_PD = new PropertyDescriptor.Builder()
            .name("text-line-count")
            .displayName("Count Lines")
            .description("If enabled, will count the number of lines present in the incoming text.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor TEXT_LINE_NONEMPTY_COUNT_PD = new PropertyDescriptor.Builder()
            .name("text-line-nonempty-count")
            .displayName("Count Non-Empty Lines")
            .description("If enabled, will count the number of lines that contain a non-whitespace character present in the incoming text.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor TEXT_WORD_COUNT_PD = new PropertyDescriptor.Builder()
            .name("text-word-count")
            .displayName("Count Words")
            .description("If enabled, will count the number of words (alphanumeric character groups bounded by whitespace)" +
                    " present in the incoming text. Common logical delimiters [_-.] do not bound a word unless 'Split Words on Symbols' is true.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor TEXT_CHARACTER_COUNT_PD = new PropertyDescriptor.Builder()
            .name("text-character-count")
            .displayName("Count Characters")
            .description("If enabled, will count the number of characters (including whitespace and symbols, but not including newlines and carriage returns) present in the incoming text.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor SPLIT_WORDS_ON_SYMBOLS_PD = new PropertyDescriptor.Builder()
            .name("split-words-on-symbols")
            .displayName("Split Words on Symbols")
            .description("If enabled, the word count will identify strings separated by common logical delimiters [ _ - . ] as independent words (ex. split-words-on-symbols = 4 words).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARACTER_ENCODING_PD = new PropertyDescriptor.Builder()
            .name("character-encoding")
            .displayName("Character Encoding")
            .description("Specifies a character encoding to use.")
            .required(true)
            .allowableValues(getStandardCharsetNames())
            .defaultValue(StandardCharsets.UTF_8.displayName())
            .build();
    public static final PropertyDescriptor ADJUST_IMMEDIATELY = new PropertyDescriptor.Builder()
            .name("ajust-immediately")
            .displayName("Call Immediate Adjustment")
            .description("If it is enabled, it will call the processSession.adjustCounter method with the \"true\" flag to adjust the counters immediately.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    private static Set<String> getStandardCharsetNames() {
        return STANDARD_CHARSETS.stream().map(c -> c.displayName()).collect(Collectors.toSet());
    }

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The flowfile contains the original content with one or more attributes added containing the respective counts")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If the flowfile text cannot be counted for some reason, the original file will be routed to this destination and nothing will be routed elsewhere")
            .build();

    private static final List<PropertyDescriptor> properties;
    private static final Set<Relationship> relationships;

    static {
        properties = Collections.unmodifiableList(Arrays.asList(TEXT_LINE_COUNT_PD,
                TEXT_LINE_NONEMPTY_COUNT_PD,
                TEXT_WORD_COUNT_PD,
                TEXT_CHARACTER_COUNT_PD,
                SPLIT_WORDS_ON_SYMBOLS_PD,
                CHARACTER_ENCODING_PD,
                ADJUST_IMMEDIATELY));

        relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS,
                REL_FAILURE)));
    }

    private volatile boolean countLines;
    private volatile boolean countLinesNonEmpty;
    private volatile boolean countWords;
    private volatile boolean countCharacters;
    private volatile boolean splitWordsOnSymbols;
    private volatile boolean adjustImmediately;
    private volatile String characterEncoding = StandardCharsets.UTF_8.name();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onSchedule(ProcessContext context) {
        this.countLines = context.getProperty(TEXT_LINE_COUNT_PD).isSet()
                ? context.getProperty(TEXT_LINE_COUNT_PD).asBoolean() : false;
        this.countLinesNonEmpty = context.getProperty(TEXT_LINE_NONEMPTY_COUNT_PD).isSet()
                ? context.getProperty(TEXT_LINE_NONEMPTY_COUNT_PD).asBoolean() : false;
        this.countWords = context.getProperty(TEXT_WORD_COUNT_PD).isSet()
                ? context.getProperty(TEXT_WORD_COUNT_PD).asBoolean() : false;
        this.countCharacters = context.getProperty(TEXT_CHARACTER_COUNT_PD).isSet()
                ? context.getProperty(TEXT_CHARACTER_COUNT_PD).asBoolean() : false;
        this.splitWordsOnSymbols = context.getProperty(SPLIT_WORDS_ON_SYMBOLS_PD).isSet()
                ? context.getProperty(SPLIT_WORDS_ON_SYMBOLS_PD).asBoolean() : false;
        this.adjustImmediately = context.getProperty(ADJUST_IMMEDIATELY).isSet()
                ? context.getProperty(ADJUST_IMMEDIATELY).asBoolean() : false;
        this.characterEncoding = context.getProperty(CHARACTER_ENCODING_PD).getValue();
    }

    /**
     * Will count text attributes of the incoming stream.
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile sourceFlowFile = processSession.get();
        if (sourceFlowFile == null) {
            return;
        }
        AtomicBoolean error = new AtomicBoolean();

        final int[] lineCount = {0};
        final int[] lineNonEmptyCount = {0};
        final int[] wordCount = {0};
        final int[] characterCount = {0};

        processSession.read(sourceFlowFile, in -> {
            long start = System.nanoTime();

            // Iterate over the lines in the text input
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in, characterEncoding));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (countLines) {
                        lineCount[0]++;
                    }

                    if (countLinesNonEmpty) {
                        if (line.trim().length() > 0) {
                            lineNonEmptyCount[0]++;
                        }
                    }

                    if (countWords) {
                        wordCount[0] += countWordsInLine(line, splitWordsOnSymbols);
                    }

                    if (countCharacters) {
                        characterCount[0] += line.length();
                    }
                }
                long stop = System.nanoTime();
                if (getLogger().isDebugEnabled()) {
                    final long durationNanos = stop - start;
                    DecimalFormat df = new DecimalFormat("#.###");
                    getLogger().debug("Computed metrics in " + durationNanos + " nanoseconds (" + df.format(durationNanos / 1_000_000_000.0) + " seconds).");
                }
                if (getLogger().isInfoEnabled()) {
                    String message = generateMetricsMessage(lineCount[0], lineNonEmptyCount[0], wordCount[0], characterCount[0]);
                    getLogger().info(message);
                }

                // Update session counters
                processSession.adjustCounter("Lines Counted", (long) lineCount[0], adjustImmediately);
                processSession.adjustCounter("Lines (non-empty) Counted", (long) lineNonEmptyCount[0], adjustImmediately);
                processSession.adjustCounter("Words Counted", (long) wordCount[0], adjustImmediately);
                processSession.adjustCounter("Characters Counted", (long) characterCount[0], adjustImmediately);
            } catch (IOException e) {
                error.set(true);
                getLogger().error(e.getMessage() + " Routing to failure.", e);
            }
        });

        if (error.get()) {
            processSession.transfer(sourceFlowFile, REL_FAILURE);
        } else {
            Map<String, String> metricAttributes = new HashMap<>();
            if (countLines) {
                metricAttributes.put(TEXT_LINE_COUNT, String.valueOf(lineCount[0]));
            }
            if (countLinesNonEmpty) {
                metricAttributes.put(TEXT_LINE_NONEMPTY_COUNT, String.valueOf(lineNonEmptyCount[0]));
            }
            if (countWords) {
                metricAttributes.put(TEXT_WORD_COUNT, String.valueOf(wordCount[0]));
            }
            if (countCharacters) {
                metricAttributes.put(TEXT_CHARACTER_COUNT, String.valueOf(characterCount[0]));
            }
            FlowFile updatedFlowFile = processSession.putAllAttributes(sourceFlowFile, metricAttributes);
            processSession.transfer(updatedFlowFile, REL_SUCCESS);
        }
    }

    private String generateMetricsMessage(int lineCount,int lineNonEmptyCount, int wordCount, int characterCount) {
        StringBuilder sb = new StringBuilder("Counted ");
        List<String> metrics = new ArrayList<>();
        if (countLines) {
            metrics.add(lineCount + " lines");
        }
        if (countLinesNonEmpty) {
            metrics.add(lineNonEmptyCount + " non-empty lines");
        }
        if (countWords) {
            metrics.add(wordCount + " words");
        }
        if (countCharacters) {
            metrics.add(characterCount + " characters");
        }
        sb.append(StringUtils.join(metrics, ", "));
        return sb.toString();
    }

    int countWordsInLine(String line, boolean splitWordsOnSymbols) throws IOException {
        if (line == null || line.trim().length() == 0) {
            return 0;
        } else {
            Pattern regex = splitWordsOnSymbols ? SYMBOL_PATTERN : WHITESPACE_ONLY_PATTERN;
            final String[] words = regex.split(line);
            // TODO: Trim individual words before counting to eliminate whitespace words?
            if (getLogger().isDebugEnabled()) {
                getLogger().debug("Split [" + line + "] to [" + StringUtils.join(Arrays.asList(words), ", ") + "] (" + words.length + ")");
            }
            return words.length;
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
