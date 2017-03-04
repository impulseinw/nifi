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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.file.monitor.LastModifiedMonitor;
import org.apache.nifi.util.file.monitor.SynchronousFileWatcher;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"scan", "attributes", "search", "lookup"})
@CapabilityDescription("Scans the specified attributes of FlowFiles, checking to see if any of their values are "
        + "present within the specified dictionary of terms")
@WritesAttributes({
    @WritesAttribute(attribute = "dictionary.hit.{n}.attribute", description = "The attribute name that had a value hit on the dictionary file."),
    @WritesAttribute(attribute = "dictionary.hit.{n}.term", description = "The term that had a hit on the dictionary file."),
    @WritesAttribute(attribute = "dictionary.hit.{n}.metadata", description = "The metadata returned from the dictionary file associated with the term hit.")
})


public class ScanAttribute extends AbstractProcessor {

    public static final String MATCH_CRITERIA_ALL = "All Must Match";
    public static final String MATCH_CRITERIA_ANY = "At Least 1 Must Match";

    public static final PropertyDescriptor MATCHING_CRITERIA = new PropertyDescriptor.Builder()
            .name("match-criteria")
            .displayName("Match Criteria")
            .description("If set to All Must Match, then FlowFiles will be routed to 'matched' only if all specified "
                    + "attributes' values are found in the dictionary. If set to At Least 1 Must Match, FlowFiles will "
                    + "be routed to 'matched' if any attribute specified is found in the dictionary")
            .required(true)
            .allowableValues(MATCH_CRITERIA_ANY, MATCH_CRITERIA_ALL)
            .defaultValue(MATCH_CRITERIA_ANY)
            .build();
    public static final PropertyDescriptor ATTRIBUTE_PATTERN = new PropertyDescriptor.Builder()
            .name("attribute-pattern")
            .displayName("Attribute Pattern")
            .description("Regular Expression that specifies the names of attributes whose values will be matched against the terms in the dictionary")
            .required(true)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .defaultValue(".*")
            .build();
    public static final PropertyDescriptor DICTIONARY_FILE = new PropertyDescriptor.Builder()
            .name("dictionary-file")
            .displayName("Dictionary File")
            .description("A new-line-delimited text file that includes the terms that should trigger a match. Empty lines are ignored.  The contents of "
                    + "the text file are loaded into memory when the processor is scheduled and reloaded when the contents are modified.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();
    public static final PropertyDescriptor DICTIONARY_FILTER = new PropertyDescriptor.Builder()
            .name("dictionary-filter-pattern")
            .displayName("Dictionary Filter Pattern")
            .description("A Regular Expression that will be applied to each line in the dictionary file. If the regular expression does not "
                    + "match the line, the line will not be included in the list of terms to search for. If a Matching Group is specified, only the "
                    + "portion of the term that matches that Matching Group will be used instead of the entire term. If not specified, all terms in "
                    + "the dictionary will be used and each term will consist of the text of the entire line in the file")
            .required(false)
            .addValidator(StandardValidators.createRegexValidator(0, 1, false))
            .defaultValue(null)
            .build();

    private static final Validator characterValidator = new StandardValidators.StringLengthValidator(1, 1);

    public static final PropertyDescriptor DICTIONARY_ENTRY_METADATA_DEMARCATOR = new PropertyDescriptor.Builder()
            .name("dictionary-entry-metadata-demarcator")
            .displayName("Dictionary Entry Metadata Demarcator")
            .description("A single character used to demarcate the dictionary entry string between dictionary value and metadata.")
            .required(false)
            .addValidator(characterValidator)
            .defaultValue(null)
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private volatile Pattern dictionaryFilterPattern = null;
    private volatile Pattern attributePattern = null;
    private volatile String dictionaryEntryMetadataDemarcator = null;
    private volatile Map<String,String> dictionaryTerms = null;
    private volatile Set<String> attributeNameMatches = null;

    private volatile SynchronousFileWatcher fileWatcher = null;

    public static final Relationship REL_MATCHED = new Relationship.Builder()
            .name("matched")
            .description("FlowFiles whose attributes are found in the dictionary will be routed to this relationship")
            .build();
    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("FlowFiles whose attributes are not found in the dictionary will be routed to this relationship")
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DICTIONARY_FILE);
        properties.add(ATTRIBUTE_PATTERN);
        properties.add(MATCHING_CRITERIA);
        properties.add(DICTIONARY_FILTER);
        properties.add(DICTIONARY_ENTRY_METADATA_DEMARCATOR);

        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_MATCHED);
        relationships.add(REL_UNMATCHED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final String filterRegex = context.getProperty(DICTIONARY_FILTER).getValue();
        this.dictionaryFilterPattern = (filterRegex == null) ? null : Pattern.compile(filterRegex);

        final String attributeRegex = context.getProperty(ATTRIBUTE_PATTERN).getValue();
        this.attributePattern = (attributeRegex.equals(".*")) ? null : Pattern.compile(attributeRegex);

        this.dictionaryTerms = createDictionary(context);
        this.fileWatcher = new SynchronousFileWatcher(Paths.get(context.getProperty(DICTIONARY_FILE).getValue()), new LastModifiedMonitor(), 1000L);

        this.dictionaryEntryMetadataDemarcator = context.getProperty(DICTIONARY_ENTRY_METADATA_DEMARCATOR).getValue();
    }

    private Map<String,String> createDictionary(final ProcessContext context) throws IOException {
        final Map<String,String> termsMeta = new HashMap<String, String>();
        this.dictionaryEntryMetadataDemarcator = context.getProperty(DICTIONARY_ENTRY_METADATA_DEMARCATOR).getValue();

        String[] termMeta;
        String term;
        String meta;


        final File file = new File(context.getProperty(DICTIONARY_FILE).getValue());
        try (final InputStream fis = new FileInputStream(file);
                final BufferedReader reader = new BufferedReader(new InputStreamReader(fis))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                if(dictionaryEntryMetadataDemarcator != null && line.contains(dictionaryEntryMetadataDemarcator)) {
                      termMeta = line.split(dictionaryEntryMetadataDemarcator);
                      term = termMeta[0];
                      meta = termMeta[1];
                } else {
                    term=line;
                    meta="";
                }

                String matchingTerm = term;
                if (dictionaryFilterPattern != null) {
                    final Matcher matcher = dictionaryFilterPattern.matcher(term);
                    if (!matcher.matches()) {
                        continue;
                    }

                    // Determine if we should use the entire line or only a part, depending on whether or not
                    // a Matching Group was specified in the regex.
                    if (matcher.groupCount() == 1) {
                        matchingTerm = matcher.group(1);
                    } else {
                        matchingTerm = term;
                    }
                }
                termsMeta.put(matchingTerm, meta);
            }
        }
        return Collections.unmodifiableMap(termsMeta);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
         List<FlowFile> flowFiles = session.get(50);
        if (flowFiles.isEmpty()) {
            return;
        }

        final ComponentLog logger = getLogger();
        try {
            if (fileWatcher.checkAndReset()) {
                this.dictionaryTerms = createDictionary(context);
            }
        } catch (final IOException e) {
            logger.error("Unable to reload dictionary due to {}", e);
        }

        final boolean matchAll = context.getProperty(MATCHING_CRITERIA).getValue().equals(MATCH_CRITERIA_ALL);

        for  (FlowFile flowFile : flowFiles) {
            final Map<String,String> matched = (matchAll ? matchAll(flowFile, attributePattern, dictionaryTerms) : matchAny(flowFile, attributePattern, dictionaryTerms));
            flowFile = session.putAllAttributes(flowFile, matched);

            final Relationship relationship = (((matched.size() == (attributeNameMatches.size() * 3) && matchAll) || (matched.size() > 0 && !matchAll))) ? REL_MATCHED : REL_UNMATCHED;
            session.getProvenanceReporter().route(flowFile, relationship);
            session.transfer(flowFile, relationship);
            logger.info("Transferred {} to {}", new Object[]{flowFile, relationship});
        }
    }

    private Map<String,String> matchAny(final FlowFile flowFile, final Pattern attributePattern, final Map<String,String> dictionary) {
        Map<String,String> dictionaryTermMatches = new HashMap<String,String>();
        attributeNameMatches = new HashSet<String>();

        int hitCounter = 0;

        for (final Map.Entry<String, String> attribute : flowFile.getAttributes().entrySet()) {
            if (attributePattern == null || attributePattern.matcher(attribute.getKey()).matches()) {
                attributeNameMatches.add(attribute.getKey());

                if (dictionary.containsKey(attribute.getValue())) {
                    hitCounter = setDictionaryTermMatch(dictionary, dictionaryTermMatches, hitCounter, attribute);
                }
            }
        }
        return dictionaryTermMatches;
    }

    private Map<String,String> matchAll(final FlowFile flowFile, final Pattern attributePattern, final Map<String,String> dictionary) {
        Map<String,String> dictionaryTermMatches = new HashMap<String,String>();
        attributeNameMatches = new HashSet<String>();

        int hitCounter = 0;

        for (final Map.Entry<String, String> attribute : flowFile.getAttributes().entrySet()) {
            if (attributePattern == null || attributePattern.matcher(attribute.getKey()).matches()) {
                attributeNameMatches.add(attribute.getKey());

                if (dictionary.containsKey(attribute.getValue())) {
                    hitCounter = setDictionaryTermMatch(dictionary, dictionaryTermMatches, hitCounter, attribute);
                } else {
                    //if one attribute value is not found in the dictionary then no need to continue since this is a matchAll scenario.
                    dictionaryTermMatches.clear();
                    break;
                }
            }
        }
        return dictionaryTermMatches;
    }

    private int setDictionaryTermMatch(Map<String, String> dictionary, Map<String, String> dictionaryTermMatches, int hitCounter, Map.Entry<String, String> attribute) {
        hitCounter++;
        dictionaryTermMatches.put("dictionary.hit." + hitCounter + ".attribute", attribute.getKey());
        dictionaryTermMatches.put("dictionary.hit." + hitCounter + ".term", attribute.getValue());
        dictionaryTermMatches.put("dictionary.hit." + hitCounter + ".metadata", dictionary.get(attribute.getValue()));
        return hitCounter;
    }
}
