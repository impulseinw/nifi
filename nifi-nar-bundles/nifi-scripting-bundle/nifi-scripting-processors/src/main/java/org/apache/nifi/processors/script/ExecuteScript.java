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
package org.apache.nifi.processors.script;


import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.script.ScriptingComponentHelper;
import org.apache.nifi.script.ScriptingComponentUtils;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.nifi.script.ScriptingComponentUtils.DYNAMIC_RELATIONSHIP_PREFIX;
import static org.apache.nifi.script.ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS;

@Tags({"script", "execute", "groovy", "python", "jython", "jruby", "ruby", "javascript", "js", "lua", "luaj", "clojure"})
@CapabilityDescription("Experimental - Executes a script given the flow file and a process session.  The script is responsible for "
        + "handling the incoming flow file (transfer to SUCCESS or remove, e.g.) as well as any flow files created by "
        + "the script. If the handling is incomplete or incorrect, the session will be rolled back. Experimental: "
        + "Impact of sustained usage not yet verified.")
@DynamicProperty(
        name = "A script engine property to update, or a dynamic relationship (if the property starts with \"REL_\" and USE_DYNAMIC_RELATIONSHIPS is set to true). "
                + "For dynamic relationships, the prefix \"REL_\" will be removed from the property name",
        value = "The value to set it to. For a dynamic relationship, the value will become the documentation of that relationship",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Updates a script engine property specified by the Dynamic Property's key with the value "
                + "specified by the Dynamic Property's value, or creates a dynamic relationship with the value becoming "
                + "the documentation of that relationship")
@DynamicRelationship(
        name = "A relationship to add",
        description = "If a dynamic property starts with \"REL_\" and USE_DYNAMIC_RELATIONSHIPS is set to true, it is assumed to be the name of a dynamic "
        + "relationship to add. In that case, the prefix \"REL_\" will be removed from the actual name of the "
        + "relationship.The corresponding dynamic property's value will become the description of the new relationship. "
        + "All (dynamic) relationships can be accessed via the script variable 'relationships', "
        + "which is a Map<String, Relationship> [name of relationship] -> relationship. "
        + "This variable will only exist, if and only if USE_DYNAMIC_RELATIONSHIPS is set to true."
)
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXECUTE_CODE,
                        explanation = "Provides operator the ability to execute arbitrary code assuming all permissions that NiFi has.")
        }
)
@InputRequirement(Requirement.INPUT_ALLOWED)
@Stateful(scopes = {Scope.LOCAL, Scope.CLUSTER},
        description = "Scripts can store and retrieve state using the State Management APIs. Consult the State Manager section of the Developer's Guide for more details.")
@SeeAlso({InvokeScriptedProcessor.class})
public class ExecuteScript extends AbstractSessionFactoryProcessor implements Searchable {

    public static final Relationship REL_SUCCESS = ScriptingComponentUtils.REL_SUCCESS;
    public static final Relationship REL_FAILURE = ScriptingComponentUtils.REL_FAILURE;

    private String scriptToRun = null;
    volatile ScriptingComponentHelper scriptingComponentHelper = new ScriptingComponentHelper();

    /** Whether to use dynamic relationships or not. */
    private volatile boolean useDynamicRelationships = Boolean.parseBoolean(USE_DYNAMIC_RELATIONSHIPS.getDefaultValue());

    /** Map to keep dynamic property keys and values.
     * They need to be stored for the case that the value of {@code useDynamicProperties} changes.
     */
    private final Map<String, String> dynamicProperties = new ConcurrentHashMap<>();

    private final Set<Relationship> relationships;
    private ComponentLog log;

    public ExecuteScript() {
        super();
        relationships = new ConcurrentSkipListSet<>();
        relationships.add(ExecuteScript.REL_SUCCESS);
        relationships.add(ExecuteScript.REL_FAILURE);
    }

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        log = getLogger();
    }

    /**
     * Returns the valid relationships for this processor.
     *
     * @return a Set of Relationships supported by this processor
     */
    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(relationships);
    }

    private Map<String, Relationship> getRelationshipsAsMap() {
        final Map<String, Relationship> relMap = new HashMap<>();
        for (final Relationship rel : relationships) {
            relMap.put(rel.getName(), rel);
        }
        return Collections.unmodifiableMap(relMap);
    }

    /**
     * Returns a list of property descriptors supported by this processor. The list always includes properties such as
     * script engine name, script file name, script body name, script arguments, and an external module path. If the
     * scripted processor also defines supported properties, those are added to the list as well.
     *
     * @return a List of PropertyDescriptor objects supported by this processor
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        initializeScriptingComponentHelper();
        return Collections.unmodifiableList(scriptingComponentHelper.getDescriptors());
    }

    /**
     * Returns a PropertyDescriptor for the given name. This is for the user to be able to define their own properties
     * which will be available as variables in the script, or to create dynamic relationships.
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return a PropertyDescriptor object corresponding to the specified dynamic property name, or the name of a dynamic relationship (without the REL_ prefix)
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final boolean isRelationship = propertySpecifiesRelationship(propertyDescriptorName);
        if (useDynamicRelationships && isRelationship) {
            return getDynamicRelationshipDescriptor(propertyDescriptorName);
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    private PropertyDescriptor getDynamicRelationshipDescriptor(final String propertyDescriptorName) {
        // we allow for arbitrary relationship names, even empty strings
        final String relName = getRelationshipName(propertyDescriptorName);
        if (!isValidRelationshipName(relName)) {
            log.warn("dynamic property for relationship is invalid: '{}'. It must not be named REL_SUCCESS or REL_FAILURE (case in-sensitive)",
                    new Object[]{propertyDescriptorName}
            );
            return new PropertyDescriptor.Builder()
                    .addValidator(new RelationshipInvalidator())
                    .dynamic(true)
                    .required(false)
                    .name(propertyDescriptorName)
                    .build();
        }
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(Validator.VALID)
                .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                .dynamic(true)
                .description(String.format("This property adds the relationship '%s'", relName))
                .build();
    }

    /**
     * Update a modified property.
     *
     * If the property signifies a dynamic relationship, the relationship will be created accordingly (or deleted if
     * {@code newValue == null}). For a dynamic relationship, the property value will become the documentation of the relationship.
     * @param descriptor The descriptor holding the dynamic property.
     * @param oldValue The previous value, or {@code null} if the property didn't exist before.
     * @param newValue The new value, or {@code null} if the property is being deleted.
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        final String descriptorName = descriptor.getName();
        if (newValue == null) {
            dynamicProperties.remove(descriptorName);
        } else {
            dynamicProperties.put(descriptorName, newValue);
        }
        if (descriptor == ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS) {
            log.debug("changing descriptor value for USE_DYNAMIC_RELATIONSHIPS to {}", new Object[]{newValue});
            this.useDynamicRelationships = Boolean.parseBoolean(newValue);
            // if using dynamic relationships is turned off, and dynamic relationships had previously been
            // created (i.e. there are more than the REL_SUCCESS and REL_FAILURE,
            // we need to remove them from the list of relationships
            if (!this.useDynamicRelationships && relationships.size() > 2) {
                // filter returns true, if relationship is dynamic
                final Predicate<? super Relationship> filter = r -> r != REL_SUCCESS && r != REL_FAILURE;
                log.debug("removing dynamic relationships from processor: [{}]",
                        new Object[]{
                                relationships.stream()
                                        .filter(filter)
                                        .map(Relationship::getName)
                                        .collect(Collectors.joining(", "))
                        });
                relationships.removeIf(filter);
            } else if (this.useDynamicRelationships) {
                // dynamic relationships have been enabled, thus we need to create them from the
                // given dynamic properties
                this.dynamicProperties.entrySet()
                        .stream()
                        .filter(e -> e.getKey().startsWith(DYNAMIC_RELATIONSHIP_PREFIX))
                        .forEach(e -> this.addDynamicRelationship(getRelationshipName(e.getKey()), e.getValue()));
            }
        } else if (this.useDynamicRelationships && propertySpecifiesRelationship(descriptorName)) {
            final String relationshipName = getRelationshipName(descriptorName);
            if (!isValidRelationshipName(relationshipName)) {
                return;
            }
            if (newValue == null) {
                relationships.removeIf(r -> relationshipName.equals(r.getName()));
                log.debug("removing relationship {}", new Object[]{relationshipName});
                return;
            }
            addDynamicRelationship(relationshipName, newValue);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        return scriptingComponentHelper.customValidate(validationContext);
    }

    /**
     * Performs setup operations when the processor is scheduled to run. This includes evaluating the processor's
     * properties, as well as reloading the script (from file or the "Script Body" property)
     *
     * @param context the context in which to perform the setup operations
     */
    @OnScheduled
    public void setup(final ProcessContext context) {
        scriptingComponentHelper.setupVariables(context);

        // Create a script engine for each possible task
        int maxTasks = context.getMaxConcurrentTasks();
        scriptingComponentHelper.setup(maxTasks, getLogger());
        scriptToRun = scriptingComponentHelper.getScriptBody();

        try {
            if (scriptToRun == null && scriptingComponentHelper.getScriptPath() != null) {
                try (final FileInputStream scriptStream = new FileInputStream(scriptingComponentHelper.getScriptPath())) {
                    scriptToRun = IOUtils.toString(scriptStream, Charset.defaultCharset());
                }
            }
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }
    }

    /**
     * Evaluates the given script body (or file) using the current session, context, and flowfile. The script
     * evaluation expects a FlowFile to be returned, in which case it will route the FlowFile to success. If a script
     * error occurs, the original FlowFile will be routed to failure. If the script succeeds but does not return a
     * FlowFile, the original FlowFile will be routed to no-flowfile
     *
     * @param context        the current process context
     * @param sessionFactory provides access to a {@link ProcessSessionFactory}, which
     *                       can be used for accessing FlowFiles, etc.
     * @throws ProcessException if the scripted processor's onTrigger() method throws an exception
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        initializeScriptingComponentHelper();
        ScriptEngine scriptEngine = scriptingComponentHelper.engineQ.poll();
        ComponentLog log = getLogger();
        if (scriptEngine == null) {
            // No engine available so nothing more to do here
            return;
        }
        ProcessSession session = sessionFactory.createSession();
        try {

            try {
                Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
                if (bindings == null) {
                    bindings = new SimpleBindings();
                }
                bindings.put("session", session);
                bindings.put("context", context);
                bindings.put("log", log);
                bindings.put("REL_SUCCESS", REL_SUCCESS);
                bindings.put("REL_FAILURE", REL_FAILURE);
                // only add map relationships if user has opted in
                if (this.useDynamicRelationships) {
                    bindings.put("relationships", getRelationshipsAsMap());
                }

                // Find the user-added properties that don't reference dynamic relationships, and set them on the script.
                // If opted out of dynamic relationship usage, all properties are passed down as variables, even those
                // starting with "REL_"
                for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                    final PropertyDescriptor property = entry.getKey();
                    // Add the dynamic entry bound to its full PropertyValue to the script engine
                    if (property.isDynamic()
                            && (!this.useDynamicRelationships || !property.getName().startsWith(DYNAMIC_RELATIONSHIP_PREFIX))
                            && entry.getValue() != null) {
                        bindings.put(entry.getKey().getName(), context.getProperty(entry.getKey()));
                    }
                }

                scriptEngine.setBindings(bindings, ScriptContext.ENGINE_SCOPE);

                // Execute any engine-specific configuration before the script is evaluated
                ScriptEngineConfigurator configurator =
                        scriptingComponentHelper.scriptEngineConfiguratorMap.get(scriptingComponentHelper.getScriptEngineName().toLowerCase());

                // Evaluate the script with the configurator (if it exists) or the engine
                if (configurator != null) {
                    configurator.eval(scriptEngine, scriptToRun, scriptingComponentHelper.getModules());
                } else {
                    scriptEngine.eval(scriptToRun);
                }

                // Commit this session for the user. This plus the outermost catch statement mimics the behavior
                // of AbstractProcessor. This class doesn't extend AbstractProcessor in order to share a base
                // class with InvokeScriptedProcessor
                session.commit();
            } catch (ScriptException e) {
                // The below 'session.rollback(true)' reverts any changes made during this session (all FlowFiles are
                // restored back to their initial session state and back to their original queues after being penalized).
                // However if the incoming relationship is full of flow files, this processor will keep failing and could
                // cause resource exhaustion. In case a user does not want to yield, it can be set to 0s in the processor
                // configuration.
                context.yield();
                throw new ProcessException(e);
            }
        } catch (final Throwable t) {
            // Mimic AbstractProcessor behavior here
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});

            // the rollback might not penalize the incoming flow file if the exception is thrown before the user gets
            // the flow file from the session binding (ff = session.get()).
            session.rollback(true);
            throw t;
        } finally {
            scriptingComponentHelper.engineQ.offer(scriptEngine);
        }
    }

    @OnStopped
    public void stop() {
        scriptingComponentHelper.stop();
    }

    @Override
    public Collection<SearchResult> search(SearchContext context) {
        Collection<SearchResult> results = new ArrayList<>();

        String term = context.getSearchTerm();

        String scriptFile = context.getProperty(ScriptingComponentUtils.SCRIPT_FILE).evaluateAttributeExpressions().getValue();
        String script = context.getProperty(ScriptingComponentUtils.SCRIPT_BODY).getValue();

        if (StringUtils.isBlank(script)) {
            try {
                script = IOUtils.toString(new FileInputStream(scriptFile), "UTF-8");
            } catch (Exception e) {
                getLogger().error(String.format("Could not read from path %s", scriptFile), e);
                return results;
            }
        }

        Scanner scanner = new Scanner(script);
        int index = 1;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            if (StringUtils.containsIgnoreCase(line, term)) {
                String text = String.format("Matched script at line %d: %s", index, line);
                results.add(new SearchResult.Builder().label(text).match(term).build());
            }
            index++;
        }

        return results;
    }

    private void initializeScriptingComponentHelper() {
        synchronized (scriptingComponentHelper.isInitialized) {
            if (!scriptingComponentHelper.isInitialized.get()) {
                scriptingComponentHelper.createResources();
                scriptingComponentHelper.addDescriptor(ScriptingComponentUtils.USE_DYNAMIC_RELATIONSHIPS);
            }
        }
    }

    private static boolean propertySpecifiesRelationship(final String propertyDescriptorName) {
        return propertyDescriptorName != null
                && propertyDescriptorName.startsWith(DYNAMIC_RELATIONSHIP_PREFIX);
    }

    private static String getRelationshipName(final String propertyDescriptorName) {
        return !propertySpecifiesRelationship(propertyDescriptorName) || DYNAMIC_RELATIONSHIP_PREFIX.equals(propertyDescriptorName)
               ? ""
               : propertyDescriptorName.substring(DYNAMIC_RELATIONSHIP_PREFIX.length());
    }

    private static boolean isValidRelationshipName(final String relationshipName) {
        final String nameInLowerCase = relationshipName.toLowerCase();
        return !(REL_SUCCESS.getName().equals(nameInLowerCase)
                || REL_FAILURE.getName().equals(nameInLowerCase));
    }

    private static class RelationshipInvalidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext validationContext) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .explanation("invalid dynamic relationship specified")
                    .valid(false)
                    .build();
        }
    }

    private void addDynamicRelationship(final String relationshipName, final String description) {
        final Relationship relationship = new Relationship.Builder()
                .name(relationshipName)
                .description(description)
                .build();
        relationships.add(relationship);
        log.debug("added dynamic relationship '{}'", new Object[] {relationshipName});
    }
}
