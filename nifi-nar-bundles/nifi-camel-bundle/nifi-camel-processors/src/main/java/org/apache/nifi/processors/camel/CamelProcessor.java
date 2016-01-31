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
package org.apache.nifi.processors.camel;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import groovy.lang.GroovyClassLoader;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.apache.camel.component.grape.GrapeCommand;
import org.apache.camel.component.grape.GrapeConstants;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.spring.SpringCamelContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.ivy.Ivy;
import org.apache.ivy.core.module.descriptor.DefaultDependencyDescriptor;
import org.apache.ivy.core.module.descriptor.DefaultModuleDescriptor;
import org.apache.ivy.core.module.id.ModuleRevisionId;
import org.apache.ivy.core.report.ArtifactDownloadReport;
import org.apache.ivy.core.report.ResolveReport;
import org.apache.ivy.core.resolve.ResolveOptions;
import org.apache.ivy.core.settings.IvySettings;
import org.apache.ivy.plugins.parser.xml.XmlModuleDescriptorWriter;
import org.apache.ivy.plugins.resolver.URLResolver;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;

/**
 * This processor runs a Camel Route.
 */
@Tags({"camel", "route", "put"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Runs a Camel Route. Each input FlowFile is converted into a Camel Exchange "
                       + "for processing by configured Route. It exports ProcessSession to camel exchange header 'nifiSession'")
public class CamelProcessor extends AbstractProcessor {

    protected static final Relationship SUCCESS = new Relationship.Builder().name("success")
        .description("Camel Route has Executed Successfully").build();

    protected static final Relationship FAILURE = new Relationship.Builder().name("failure")
        .description("Camel Route has Failed to Execute").build();

    public static final PropertyDescriptor CAMEL_SPRING_CONTEXT_FILE_PATH = new PropertyDescriptor.Builder()
        .name("Camel Spring Config File Path")
        .description("The Classpath where NiFi can find Spring Application context file"
                         + " Ex: classpath*:/META-INF/camel-application-context.xml")
        .defaultValue("").addValidator(Validator.VALID)
        .build();

    public static final PropertyDescriptor CAMEL_SPRING_CONTEXT_DEF = new PropertyDescriptor.Builder()
    .name("Camel Spring Context Definition")
    .description("Content of Spring Application context ")
    .defaultValue("").addValidator(Validator.VALID)
    .build();

    public static final PropertyDescriptor CAMEL_ENTRY_POINT_URI = new PropertyDescriptor.Builder()
        .name("Camel EntryPoint")
        .description("EntryPoint for NiFi in Camel Route" + " Ex: direct-vm:nifiEntryPoint")
        .defaultValue("direct-vm:nifiEntryPoint").required(true).addValidator(Validator.VALID).build();

    /**
     *@see <a href="http://camel.apache.org/grape.html">  Camel Grape Documentation</a>
     */
    public static final PropertyDescriptor EXT_LIBRARIES = new PropertyDescriptor.Builder()
    .name("Extra Libraries")
    .description("Comma Seperated List of Extra Libraries/Features to Download and Use [ in GroupId/ArtifactId/version format]. "
        + "Ex: org.apache.camel/camel-mail/2.16.1,\n"
        + "org.apache.camel/camel-infinispan/2.16.1")
    .required(false).addValidator(GrapeGrabValidator.INSTANCE).build();

    private  SpringCamelContext camelContext = null;

    private ImmutableList<PropertyDescriptor> descriptors;

    private ImmutableSet<Relationship> relationships=ImmutableSet.of(SUCCESS, FAILURE);

    private synchronized SpringCamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        CamelContext camelContext=getCamelContext();
        Exchange exchange = new DefaultExchange(camelContext);
        exchange.getIn().setBody(flowFile);
        exchange.getIn().setHeader("nifiSession", session);
        ProducerTemplate producerTemplate= camelContext.createProducerTemplate();
        producerTemplate.setDefaultEndpointUri(context.getProperty(CAMEL_ENTRY_POINT_URI)
                                                   .getValue());
        exchange = producerTemplate.send(exchange);
        try{
            producerTemplate.stop();
        }catch(Exception e){
            throw new ProcessException(e);
        }
        if (exchange != null && !(exchange.isFailed())) {
            session.transfer(exchange.getIn().getBody(FlowFile.class), SUCCESS);
        } else {
            if (exchange.isFailed() && exchange.getException() != null) {
                session.putAttribute(flowFile, "camelRouteException", exchange.getException().getMessage());
            }
            session.transfer(flowFile, FAILURE);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = ImmutableList.of(CAMEL_SPRING_CONTEXT_FILE_PATH, CAMEL_SPRING_CONTEXT_DEF,
                                            CAMEL_ENTRY_POINT_URI,EXT_LIBRARIES);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws Exception {
        if (getCamelContext() == null
            || getCamelContext().getStatus()==ServiceStatus.Stopped
            || getCamelContext().getStatus()==ServiceStatus.Stopping) {
            try {
                String camelContextDef=context.getProperty(CAMEL_SPRING_CONTEXT_DEF).getValue();
                String camelContextPath=context.getProperty(CAMEL_SPRING_CONTEXT_FILE_PATH).getValue();

                boolean contextDefined=!(Strings.isNullOrEmpty(camelContextDef) && Strings.isNullOrEmpty(camelContextPath));
                if(contextDefined){
                    final GroovyClassLoader classLoader=new GroovyClassLoader(Thread.currentThread().getContextClassLoader());
                    final String grapeGrabURLs=context.getProperty(EXT_LIBRARIES).getValue();
                    //Let's load Extra Libraries those might not be present in classpath.
                    List<File> resolvedDependencies=null;
                    if(!StringUtils.isEmpty(grapeGrabURLs)){
                        resolvedDependencies=DefaultArtifactResolver.INSTANCE.resolveArtifact(grapeGrabURLs.split(","));
                        for (File file : resolvedDependencies) {
                            classLoader.addClasspath(file.getAbsolutePath());
                        }
                    }
                GenericXmlApplicationContext applicationContext=new GenericXmlApplicationContext();
                applicationContext.setClassLoader(classLoader);
                if(!Strings.isNullOrEmpty(camelContextDef)){
                    applicationContext.load(
                         new ByteArrayResource(camelContextDef.getBytes()));
                }else if(!Strings.isNullOrEmpty(camelContextPath)){
                    applicationContext.load(camelContextPath);
                }
                applicationContext.refresh();
                camelContext=new SpringCamelContext(applicationContext);
                camelContext.start();
                getLogger().info("Camel Spring Context initialized: " + camelContext.getName());
                }
            } catch (Exception exception) {
                getLogger().error("Failed to Startup Camel Spring Context", exception);
                throw exception;
            }
        }

    }

    @OnStopped
    public void stopped(){
        if (getCamelContext() != null && getCamelContext().getApplicationContext()!=null) {
            try {
                ProducerTemplate template=getCamelContext().createProducerTemplate();
                template.sendBodyAndHeader("grape:grape","Clear Downloaded Dependencies", GrapeConstants.getGRAPE_COMMAND(), GrapeCommand.clearPatches);
                template.stop();
                getCamelContext().stop();
                getCamelContext().destroy();
            } catch (Exception e) {
               getLogger().error("Failed to Shutdown Camel Spring Context", e);
            }finally{
                ((AbstractApplicationContext)getCamelContext().getApplicationContext()).close();
            }
        }
    }

    /**
     * To validate {@link groovy.lang.Grab Grab} URLs for {@link groovy.grape.Grape Grape}.
     * @see <a href="http://camel.apache.org/grape.html">  Camel Grape Documentation</a>
     */
    enum GrapeGrabValidator implements Validator {
        INSTANCE;

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if(!StringUtils.isEmpty(input)){
            final String[] grapeGrabURLs = input.split(",");
            for (final String grapeGrabURL : grapeGrabURLs) {
                String [] eitherOfGAVs=grapeGrabURL.split("/");
                String validationError=null;
                if(eitherOfGAVs.length!=3){
                    validationError="Pattern Should be in Group/Artifact/Version Format.";
                }else{
                    if(!eitherOfGAVs[2].equalsIgnoreCase("default")
                        && !NumberUtils.isDigits(eitherOfGAVs[2].replaceAll("\\.", ""))){
                        validationError="Version number Should be dotted digits or default";
                    }
                }
                if(validationError!=null){
                    return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(validationError).valid(false).build();
                }
            }
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    }

    enum DefaultArtifactResolver implements ArtifactResolver {
        INSTANCE;
        @Override
        public List<File> resolveArtifact(String ... gavs) throws IOException,
        ParseException {
            // creates clear ivy settings
            IvySettings ivySettings = new IvySettings();
            // url resolver for configuration of maven repo
            URLResolver resolver = new URLResolver();
            resolver.setM2compatible(true);
            resolver.setName("central");
            // you can specify the url resolution pattern strategy
            resolver
            .addArtifactPattern("http://repo1.maven.org/maven2/[organisation]/[module]/[revision]/[artifact](-[revision]).[ext]");
            // adding maven repo resolver
            ivySettings.addResolver(resolver);
            // set to the default resolver
            ivySettings.setDefaultResolver(resolver.getName());
            // creates an Ivy instance with settings
            Ivy ivy = Ivy.newInstance(ivySettings);
            File ivyfile = File.createTempFile("ivy", ".xml");
            ivyfile.deleteOnExit();
            DefaultModuleDescriptor md = DefaultModuleDescriptor.newDefaultInstance(ModuleRevisionId
                                                                                    .newInstance("org.apache.nifi", "camelprocessor-caller", "working"));
            for (String gav : gavs) {
            String[] dep = gav.split("/");
            DefaultDependencyDescriptor dd = new DefaultDependencyDescriptor(
                                                                             md,
                                                                             ModuleRevisionId.newInstance(dep[0],
                                                                                                          dep[1],
                                                                                                          dep[2]),
                                                                                                          false, false, true);
            md.addDependency(dd);
            }
            // creates an ivy configuration file
            XmlModuleDescriptorWriter.write(md, ivyfile);
            String[] confs = new String[] {"default"};
            ResolveOptions resolveOptions = new ResolveOptions().setConfs(confs);
            // init resolve report
            ResolveReport report = ivy.resolve(ivyfile.toURL(), resolveOptions);
            // so you can get the jar library
            List<File> jarArtifactFiles = new LinkedList<>();
            for (ArtifactDownloadReport artifactDownloadReport : report.getAllArtifactsReports()) {
                jarArtifactFiles.add(artifactDownloadReport.getLocalFile());
            }
            return jarArtifactFiles;
        }
    }
}