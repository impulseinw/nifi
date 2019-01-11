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
package org.apache.nifi.fn.core;

import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileAccessException;
import org.apache.nifi.processor.exception.FlowFileHandlingException;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceReporter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FnProcessSession implements ProcessSession {

    private final boolean materializeContent;
    private final Map<Relationship, Queue<FnFlowFile>> outputMap = new HashMap<>();
    private final Queue<FnFlowFile> inputQueue;
    private final Set<Long> beingProcessed = new HashSet<>();
    private final List<FnFlowFile> penalized = new ArrayList<>();
    private final Processor processor;

    private final Map<Long, FnFlowFile> currentVersions = new HashMap<>();
    private final Map<Long, FnFlowFile> originalVersions = new HashMap<>();
    private final Map<String, Long> counterMap = new HashMap<>();
    private final ProvenanceCollector provenanceReporter;

    private boolean committed = false;
    private boolean rolledback = false;
    private final Set<Long> removedFlowFiles = new HashSet<>();

    private static final AtomicLong enqueuedIndex = new AtomicLong(0L);
    private final Runnable nextStep; //run before commit() completes

    public FnProcessSession(final Queue<FnFlowFile> input, Collection<ProvenanceEventRecord> events, final Processor processor, Set<Relationship> outputRelationships, boolean materializeContent,
                            Runnable nextStep) {
        this.processor = processor;
        this.inputQueue = input;
        this.provenanceReporter = new ProvenanceCollector(this, events, processor.getIdentifier(), processor.getClass().getSimpleName());
        this.materializeContent = materializeContent;
        this.nextStep = nextStep;
        outputRelationships.forEach(r -> outputMap.put(r, new LinkedList<>()));
    }

    //region Attributes

    @Override
    public FnFlowFile putAllAttributes(FlowFile flowFile, final Map<String, String> attrs) {
        flowFile = validateState(flowFile);
        if (attrs == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot update attributes of a flow file that I did not create");
        }
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.putAttributes(attrs);
        return newFlowFile;
    }

    @Override
    public FnFlowFile putAttribute(FlowFile flowFile, final String attrName, final String attrValue) {
        flowFile = validateState(flowFile);
        if (attrName == null || attrValue == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot update attributes of a flow file that I did not create");
        }

        if ("uuid".equals(attrName)) {
            throw new IllegalArgumentException("Should not be attempting to set FlowFile UUID via putAttribute");
        }

        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Map<String, String> attrs = new HashMap<>();
        attrs.put(attrName, attrValue);
        newFlowFile.putAttributes(attrs);
        return newFlowFile;
    }

    @Override
    public FnFlowFile removeAllAttributes(FlowFile flowFile, final Set<String> attrNames) {
        flowFile = validateState(flowFile);
        if (attrNames == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.removeAttributes(attrNames);
        return newFlowFile;
    }

    @Override
    public FnFlowFile removeAllAttributes(FlowFile flowFile, final Pattern keyPattern) {
        flowFile = validateState(flowFile);
        if (flowFile == null) {
            throw new IllegalArgumentException("flowFile cannot be null");
        }
        if (keyPattern == null) {
            return (FnFlowFile) flowFile;
        }

        final Set<String> attrsToRemove = new HashSet<>();
        for (final String key : flowFile.getAttributes().keySet()) {
            if (keyPattern.matcher(key).matches()) {
                attrsToRemove.add(key);
            }
        }

        return removeAllAttributes(flowFile, attrsToRemove);
    }

    @Override
    public FnFlowFile removeAttribute(FlowFile flowFile, final String attrName) {
        flowFile = validateState(flowFile);
        if (attrName == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        final Set<String> attrNames = new HashSet<>();
        attrNames.add(attrName);
        newFlowFile.removeAttributes(attrNames);
        return newFlowFile;
    }

    /**
     * Inherits the attributes from the given source flow file into another flow
     * file. The UUID of the source becomes the parent UUID of this flow file.
     * If a parent uuid had previously been established it will be replaced by
     * the uuid of the given source
     *
     * @param source the FlowFile from which to copy attributes
     * @param destination the FlowFile to which to copy attributes
     */
    private FlowFile inheritAttributes(final FlowFile source, final FlowFile destination) {
        if (source == null || destination == null || source == destination) {
            return destination; // don't need to inherit from ourselves
        }
        final FlowFile updated = putAllAttributes(destination, source.getAttributes());
        getProvenanceReporter().fork(source, Collections.singletonList(updated));
        return updated;
    }

    /**
     * Inherits the attributes from the given source flow files into the
     * destination flow file. The UUIDs of the sources becomes the parent UUIDs
     * of the destination flow file. Only attributes which is common to all
     * source items is copied into this flow files attributes. Any previously
     * established parent UUIDs will be replaced by the UUIDs of the sources. It
     * will capture the uuid of a certain number of source objects and may not
     * capture all of them. How many it will capture is unspecified.
     *
     * @param sources to inherit common attributes from
     */
    private FlowFile inheritAttributes(final Collection<FlowFile> sources, final FlowFile destination) {
        final StringBuilder parentUuidBuilder = new StringBuilder();
        int uuidsCaptured = 0;
        for (final FlowFile source : sources) {
            if (source == destination) {
                continue; // don't want to capture parent uuid of this. Something can't be a child of itself
            }
            final String sourceUuid = source.getAttribute(CoreAttributes.UUID.key());
            if (sourceUuid != null && !sourceUuid.trim().isEmpty()) {
                uuidsCaptured++;
                if (parentUuidBuilder.length() > 0) {
                    parentUuidBuilder.append(",");
                }
                parentUuidBuilder.append(sourceUuid);
            }

            if (uuidsCaptured > 100) {
                break;
            }
        }

        final FlowFile updated = putAllAttributes(destination, intersectAttributes(sources));
        getProvenanceReporter().join(sources, updated);
        return updated;
    }

    /**
     * Returns the attributes that are common to every flow file given. The key
     * and value must match exactly.
     *
     * @param flowFileList a list of flow files
     *
     * @return the common attributes
     */
    private static Map<String, String> intersectAttributes(final Collection<FlowFile> flowFileList) {
        final Map<String, String> result = new HashMap<>();
        // trivial cases
        if (flowFileList == null || flowFileList.isEmpty()) {
            return result;
        } else if (flowFileList.size() == 1) {
            result.putAll(flowFileList.iterator().next().getAttributes());
        }

        /*
         * Start with the first attribute map and only put an entry to the
         * resultant map if it is common to every map.
         */
        final Map<String, String> firstMap = flowFileList.iterator().next().getAttributes();

        outer:
        for (final Map.Entry<String, String> mapEntry : firstMap.entrySet()) {
            final String key = mapEntry.getKey();
            final String value = mapEntry.getValue();
            for (final FlowFile flowFile : flowFileList) {
                final Map<String, String> currMap = flowFile.getAttributes();
                final String curVal = currMap.get(key);
                if (curVal == null || !curVal.equals(value)) {
                    continue outer;
                }
            }
            result.put(key, value);
        }

        return result;
    }

    //endregion

    //region Metadata
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void migrate(final ProcessSession newOwner, final Collection<FlowFile> flowFiles) {
        Collection<FnFlowFile> fnFlowFiles = (Collection<FnFlowFile>) (Collection) flowFiles;
        FnProcessSession newFnOwner = (FnProcessSession) newOwner;
        if (Objects.requireNonNull(newOwner) == this) {
            throw new IllegalArgumentException("Cannot migrate FlowFiles from a Process Session to itself");
        }
        if (flowFiles == null || flowFiles.isEmpty()) {
            throw new IllegalArgumentException("Must supply at least one FlowFile to migrate");
        }

        if (!(newOwner instanceof FnProcessSession)) {
            throw new IllegalArgumentException("Cannot migrate from a org.apache.nifi.fn.core.FnProcessSession to a session of type " + newOwner.getClass());
        }
        for (final FnFlowFile flowFile : fnFlowFiles) {

            final FnFlowFile currentVersion = currentVersions.get(flowFile.getId());
            if (currentVersion == null) {
                throw new FlowFileHandlingException(flowFile + " is not known in this session");
            }
        }

        for (final Map.Entry<Relationship, Queue<FnFlowFile>> entry : outputMap.entrySet()) {
            final Relationship relationship = entry.getKey();
            final Queue<FnFlowFile> transferredFlowFiles = entry.getValue();

            for (final FnFlowFile flowFile : fnFlowFiles) {
                if (transferredFlowFiles.remove(flowFile)) {
                    newFnOwner.outputMap.computeIfAbsent(relationship, rel -> new LinkedList<>()).add(flowFile);
                }
            }
        }

        for (final FnFlowFile flowFile : fnFlowFiles) {
            if (beingProcessed.remove(flowFile.getId())) {
                newFnOwner.beingProcessed.add(flowFile.getId());
            }

            if (penalized.remove(flowFile)) {
                newFnOwner.penalized.add(flowFile);
            }

            if (currentVersions.containsKey(flowFile.getId())) {
                newFnOwner.currentVersions.put(flowFile.getId(), currentVersions.remove(flowFile.getId()));
            }

            if (originalVersions.containsKey(flowFile.getId())) {
                newFnOwner.originalVersions.put(flowFile.getId(), originalVersions.remove(flowFile.getId()));
            }

            if (removedFlowFiles.remove(flowFile.getId())) {
                newFnOwner.removedFlowFiles.add(flowFile.getId());
            }
        }

        final Set<String> flowFileIds = flowFiles.stream()
            .map(ff -> ff.getAttribute(CoreAttributes.UUID.key()))
            .collect(Collectors.toSet());

        provenanceReporter.migrate(newFnOwner.provenanceReporter, flowFileIds);
    }

    @Override
    public void adjustCounter(final String name, final long delta, final boolean immediate) {
        if (immediate) {
            //sharedState.adjustCounter(name, delta);
            //return;
        }

        Long counter = counterMap.get(name);
        if (counter == null) {
            counter = delta;
            counterMap.put(name, counter);
            return;
        }

        counter = counter + delta;
        counterMap.put(name, counter);
    }

    @Override
    public void remove(FlowFile flowFile) {
        flowFile = validateState(flowFile);

        final Iterator<FnFlowFile> penalizedItr = penalized.iterator();
        while (penalizedItr.hasNext()) {
            final FnFlowFile ff = penalizedItr.next();
            if (Objects.equals(ff.getId(), flowFile.getId())) {
                penalizedItr.remove();
                penalized.remove(ff);
                break;
            }
        }

        final Iterator<Long> processedItr = beingProcessed.iterator();
        while (processedItr.hasNext()) {
            final Long ffId = processedItr.next();
            if (ffId != null && ffId.equals(flowFile.getId())) {
                processedItr.remove();
                beingProcessed.remove(ffId);
                removedFlowFiles.add(flowFile.getId());
                currentVersions.remove(ffId);
                return;
            }
        }

        throw new ProcessException(flowFile + " not found in queue");
    }

    @Override
    public void remove(Collection<FlowFile> flowFiles) {
        flowFiles = validateState(flowFiles);

        for (final FlowFile flowFile : flowFiles) {
            remove(flowFile);
        }
    }

    @Override
    public void rollback() {
        rollback(false);
    }

    @Override
    public void rollback(final boolean penalize) {
        //if we've already committed then rollback is basically a no-op
        if (committed) {
            return;
        }

        for (final Queue<FnFlowFile> list : outputMap.values()) {
            for (final FnFlowFile flowFile : list) {
                inputQueue.offer(flowFile);
                if (penalize) {
                    penalized.add(flowFile);
                }
            }
        }

        for (final Long flowFileId : beingProcessed) {
            final FnFlowFile flowFile = originalVersions.get(flowFileId);
            if (flowFile != null) {
                inputQueue.offer(flowFile);
                if (penalize) {
                    penalized.add(flowFile);
                }
            }
        }

        rolledback = true;
        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
        outputMap.clear();
        clearTransferState();
        if (!penalize) {
            penalized.clear();
        }
    }

    @Override
    public void transfer(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("I only accept org.apache.nifi.fn.core.FnFlowFile");
        }

        // if the flowfile provided was created in this session (i.e. it's in currentVersions and not in original versions),
        // then throw an exception indicating that you can't transfer flowfiles back to self.
        // this mimics the same behavior in StandardProcessSession
        if (currentVersions.get(flowFile.getId()) != null && originalVersions.get(flowFile.getId()) == null) {
            throw new IllegalArgumentException("Cannot transfer FlowFiles that are created in this Session back to self");
        }

        beingProcessed.remove(flowFile.getId());
        inputQueue.add((FnFlowFile) flowFile);
        updateLastQueuedDate((FnFlowFile) flowFile);

    }

    @Override
    public void transfer(final Collection<FlowFile> flowFiles) {
        flowFiles.forEach(this::transfer);
    }

    @Override
    public void transfer(FlowFile flowFile, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFile);
            return;
        }
        if (!processor.getRelationships().contains(relationship)) {
            throw new IllegalArgumentException("this relationship " + relationship.getName() + " is not known");
        }

        flowFile = validateState(flowFile);

        if (outputMap.containsKey(relationship)) {
            Queue<FnFlowFile> queue = this.outputMap.get(relationship);
            queue.add((FnFlowFile) flowFile);

        }
        beingProcessed.remove(flowFile.getId());
        updateLastQueuedDate((FnFlowFile) flowFile);
    }

    @Override
    public void transfer(Collection<FlowFile> flowFiles, final Relationship relationship) {
        if (relationship == Relationship.SELF) {
            transfer(flowFiles);
            return;
        }
        for (final FlowFile flowFile : flowFiles) {
            transfer(flowFile, relationship);
        }
    }

    @Override
    public ProvenanceReporter getProvenanceReporter() {
        return provenanceReporter;
    }

    @Override
    public FnFlowFile penalize(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(true);
        penalized.add(newFlowFile);
        return newFlowFile;
    }

    @Override
    public FnFlowFile create() {
        final FnFlowFile flowFile = new FnFlowFile(this.materializeContent);
        currentVersions.put(flowFile.getId(), flowFile);
        beingProcessed.add(flowFile.getId());
        return flowFile;
    }

    @Override
    public FnFlowFile create(final FlowFile flowFile) {
        FnFlowFile newFlowFile = create();
        newFlowFile = (FnFlowFile) inheritAttributes(flowFile, newFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        return newFlowFile;
    }

    @Override
    public FnFlowFile create(final Collection<FlowFile> flowFiles) {
        FnFlowFile newFlowFile = create();
        newFlowFile = (FnFlowFile) inheritAttributes(flowFiles, newFlowFile);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        return newFlowFile;
    }

    @Override
    public FnFlowFile get() {
        final FnFlowFile flowFile = inputQueue.poll();
        if (flowFile != null) {
            beingProcessed.add(flowFile.getId());
            currentVersions.put(flowFile.getId(), flowFile);
            originalVersions.put(flowFile.getId(), flowFile);
        }
        return flowFile;
    }

    @Override
    public List<FlowFile> get(final int maxResults) {
        final List<FlowFile> flowFiles = new ArrayList<>(Math.min(500, maxResults));
        for (int i = 0; i < maxResults; i++) {
            final FnFlowFile nextFlowFile = get();
            if (nextFlowFile == null) {
                return flowFiles;
            }

            flowFiles.add(nextFlowFile);
        }

        return flowFiles;
    }

    @Override
    public List<FlowFile> get(final FlowFileFilter filter) {
        final List<FlowFile> flowFiles = new ArrayList<>();
        final List<FnFlowFile> unselected = new ArrayList<>();

        while (true) {
            final FnFlowFile flowFile = inputQueue.poll();
            if (flowFile == null) {
                break;
            }

            final FlowFileFilter.FlowFileFilterResult filterResult = filter.filter(flowFile);
            if (filterResult.isAccept()) {
                flowFiles.add(flowFile);

                beingProcessed.add(flowFile.getId());
                currentVersions.put(flowFile.getId(), flowFile);
                originalVersions.put(flowFile.getId(), flowFile);
            } else {
                unselected.add(flowFile);
            }

            if (!filterResult.isContinue()) {
                break;
            }
        }

        inputQueue.addAll(unselected);
        return flowFiles;
    }

    @Override
    public QueueSize getQueueSize() {

        final int count = inputQueue.size();

        long contentSize = 0L;
        for (final FnFlowFile flowFile : inputQueue) {
            contentSize += flowFile.getSize();
        }
        return new QueueSize(count, contentSize);
    }

    //endregion

    @Override
    public void commit() {
        if (!beingProcessed.isEmpty()) {
            throw new FlowFileHandlingException("Cannot commit session because the following FlowFiles have not been removed or transferred: " + beingProcessed);
        }
        committed = true;

        this.nextStep.run();

        beingProcessed.clear();
        currentVersions.clear();
        originalVersions.clear();
    }

    //region Content
    @Override
    public FnFlowFile clone(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        return newFlowFile;
    }

    @Override
    public FnFlowFile clone(FlowFile flowFile, final long offset, final long size) {
        flowFile = validateState(flowFile);
        try {
            ((FnFlowFile) flowFile).materializeData();
        } catch (IOException e) {
            e.printStackTrace();
            throw new FlowFileHandlingException("Error materializing data", e);

        }
        if (offset + size > flowFile.getSize()) {
            throw new FlowFileHandlingException("Specified offset of " + offset + " and size " + size + " exceeds size of " + flowFile.toString());
        }

        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, offset, size, this.materializeContent);

        currentVersions.put(newFlowFile.getId(), newFlowFile);
        beingProcessed.add(newFlowFile.getId());
        return newFlowFile;
    }

    @Override
    public void exportTo(FlowFile flowFile, final OutputStream out) {
        flowFile = validateState(flowFile);
        if (flowFile == null || out == null) {
            throw new IllegalArgumentException("arguments cannot be null");
        }

        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        try {
            copyTo(((FnFlowFile) flowFile).getDataStream(), out);
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public void exportTo(FlowFile flowFile, final Path path, final boolean append) {
        flowFile = validateState(flowFile);
        if (flowFile == null || path == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        FnFlowFile fnFlowFile = (FnFlowFile) flowFile;

        final OpenOption mode = append ? StandardOpenOption.APPEND : StandardOpenOption.CREATE;

        try (final OutputStream out = Files.newOutputStream(path, mode)) {
            if (fnFlowFile.materializeContent)
                fnFlowFile.materializeData();
            copyTo(fnFlowFile.getDataStream(), out);
        } catch (final IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
    }

    @Override
    public FnFlowFile importFrom(final InputStream in, FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (in == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        newFlowFile.setData(in);

        currentVersions.put(newFlowFile.getId(), newFlowFile);

        return newFlowFile;
    }

    @Override
    public FnFlowFile importFrom(final Path path, final boolean keepSourceFile, FlowFile flowFile) {
        flowFile = validateState(flowFile);
        if (path == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        if (keepSourceFile == false) {
            throw new IllegalArgumentException("Not going to delete the file...");
        }
        FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        try {
            newFlowFile.setData(Files.newInputStream(path));
        } catch (IOException e) {
            throw new FlowFileAccessException(e.toString(), e);
        }
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile = putAttribute(newFlowFile, CoreAttributes.FILENAME.key(), path.getFileName().toString());
        return newFlowFile;
    }

    @Override
    public void read(final FlowFile flowFile, final InputStreamCallback callback) {
        read(flowFile, false, callback);
    }

    @Override
    public void read(FlowFile flowFile, boolean allowSessionStreamManagement, final InputStreamCallback callback) {
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }

        flowFile = validateState(flowFile);
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        //allowSessionStreamManagement not used...
        try {
            ((FnFlowFile) flowFile).materializeData();
            callback.process(((FnFlowFile) flowFile).getDataStream());
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }
    }

    @Override
    public InputStream read(FlowFile flowFile) {
        flowFile = validateState(flowFile);

        return ((FnFlowFile) flowFile).getDataStream();
    }

    @Override
    public FnFlowFile write(FlowFile flowFile, final OutputStreamCallback callback) {
        flowFile = validateState(flowFile);
        if (callback == null) {
            throw new IllegalArgumentException("callback cannot be null");
        }
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            callback.process(baos);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }

        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        newFlowFile.setData(baos.toByteArray());
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        return newFlowFile;
    }

    @Override
    public OutputStream write(FlowFile flowFile) {
        if (!(flowFile instanceof FnFlowFile)) {
            throw new IllegalArgumentException("Cannot export a flow file that I did not create");
        }

        final FnFlowFile FnFlowFile = validateState(flowFile);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, materializeContent);
                currentVersions.put(newFlowFile.getId(), newFlowFile);
            }
        };

        return baos;
    }

    @Override
    public FlowFile append(FlowFile flowFile, final OutputStreamCallback callback) {
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        final FnFlowFile validatedFlowFile = validateState(flowFile);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            callback.process(baos);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }

        final FnFlowFile newFlowFile = new FnFlowFile(validatedFlowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        newFlowFile.addData(baos.toByteArray());
        return newFlowFile;
    }

    @Override
    public FnFlowFile write(final FlowFile flowFile, final StreamCallback callback) {
        if (callback == null || flowFile == null) {
            throw new IllegalArgumentException("argument cannot be null");
        }
        final FnFlowFile fnFlowFile = validateState(flowFile);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            callback.process((fnFlowFile).getDataStream(), out);
        } catch (final IOException e) {
            throw new ProcessException(e.toString(), e);
        }

        final FnFlowFile newFlowFile = new FnFlowFile(fnFlowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setData(out.toByteArray());

        return newFlowFile;
    }

    @Override
    public FnFlowFile merge(Collection<FlowFile> sources, FlowFile destination) {
        sources = validateState(sources);
        destination = validateState(destination);
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) destination, this.materializeContent);
        for (final FlowFile flowFile : sources) {
            newFlowFile.addData(((FnFlowFile) flowFile).getDataStream());
        }
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        return newFlowFile;
    }

    @Override
    public FnFlowFile merge(Collection<FlowFile> sources, FlowFile destination, byte[] header, byte[] footer, byte[] demarcator) {
        Collection<FnFlowFile> fnSources = (Collection) validateState(sources);
        FnFlowFile fnDestination = validateState(destination);

        if (header != null) {
            fnDestination.addData(header);
        }

        int count = 0;
        for (final FnFlowFile flowFile : fnSources) {
            fnDestination.addData(flowFile.getDataStream());
            if (demarcator != null && ++count != sources.size()) {
                fnDestination.addData(demarcator);
            }
        }

        if (footer != null) {
            fnDestination.addData(footer);
        }

        final FnFlowFile newFlowFile = new FnFlowFile(fnDestination, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);

        return newFlowFile;
    }

    public FnFlowFile unpenalize(FlowFile flowFile) {
        flowFile = validateState(flowFile);
        final FnFlowFile newFlowFile = new FnFlowFile((FnFlowFile) flowFile, this.materializeContent);
        currentVersions.put(newFlowFile.getId(), newFlowFile);
        newFlowFile.setPenalized(false);
        penalized.remove(newFlowFile);
        return newFlowFile;
    }
    //endregion

    //region Utility

    boolean isFlowFileKnown(final FlowFile flowFile) {
        final FlowFile curFlowFile = currentVersions.get(flowFile.getId());
        if (curFlowFile == null) {
            return false;
        }

        final String curUuid = curFlowFile.getAttribute(CoreAttributes.UUID.key());
        final String providedUuid = curFlowFile.getAttribute(CoreAttributes.UUID.key());
        if (!curUuid.equals(providedUuid)) {
            return false;
        }

        return true;
    }

    private List<FlowFile> validateState(final Collection<FlowFile> flowFiles) {
        return flowFiles.stream()
            .map(ff -> validateState(ff))
            .collect(Collectors.toList());
    }

    private FnFlowFile validateState(final FlowFile flowFile) {
        Objects.requireNonNull(flowFile);

        final FnFlowFile currentVersion = currentVersions.get(flowFile.getId());
        if (currentVersion == null) {
            throw new FlowFileHandlingException(flowFile + " is not known in this session");
        }

        for (final Queue<FnFlowFile> flowFiles : outputMap.values()) {
            if (flowFiles.contains(flowFile)) {
                throw new IllegalStateException(flowFile + " has already been transferred");
            }
        }

        return currentVersion;
    }

    public boolean isCommitted() {
        return committed;
    }

    public boolean isRolledback() {
        return rolledback;
    }

    public boolean isInputQueueEmpty() {
        return this.inputQueue.isEmpty();
    }

    public boolean areAllFlowFilesTransfered(final Relationship relationship) {
        if (outputMap.containsKey(relationship)) {
            if (!outputMap.get(relationship).isEmpty())
                return false;
        }
        return true;
    }

    public void clearTransferState() {
        this.outputMap.clear();
    }

    public int getRemovedCount() {
        return removedFlowFiles.size();
    }

    public Queue<FnFlowFile> getAndRemoveFlowFilesForRelationship(final String relationship) {
        final Relationship procRel = new Relationship.Builder().name(relationship).build();
        return getAndRemoveFlowFilesForRelationship(procRel);
    }

    public Queue<FnFlowFile> getAndRemoveFlowFilesForRelationship(final Relationship relationship) {
        Queue<FnFlowFile> queue = this.outputMap.get(relationship);
        if (queue == null) {
            queue = new LinkedList<>();
        }
        this.outputMap.remove(relationship);

        return queue;
    }

    public List<FnFlowFile> getPenalizedFlowFiles() {
        return penalized;
    }

    private void updateLastQueuedDate(FnFlowFile FnFlowFile) {
        // Simulate StandardProcessSession.updateLastQueuedDate,
        // which is called when a flow file is transferred to a relationship.
        FnFlowFile.setLastEnqueuedDate(System.currentTimeMillis());
        FnFlowFile.setEnqueuedIndex(enqueuedIndex.incrementAndGet());
    }

    private void copyTo(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024 * 1024];
        int len;
        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }
    }
    //endregion
}
