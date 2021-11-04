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
package org.apache.nifi.processors.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsCreateModes;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;
import org.ietf.jgss.GSSException;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This processor copies FlowFiles to HDFS.
 */
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "HCFS", "HDFS", "put", "copy", "filesystem"})
@CapabilityDescription("Write FlowFile data to Hadoop Distributed File System (HDFS)")
@ReadsAttribute(attribute = "filename", description = "The name of the file written to HDFS comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file written to HDFS is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file on HDFS is stored in this attribute."),
        @WritesAttribute(attribute = "target.dir.created", description = "The result(true/false) indicates if the folder is created by the processor.")
})
@SeeAlso(GetHDFS.class)
@Restricted(restrictions = {
    @Restriction(
        requiredPermission = RequiredPermission.WRITE_DISTRIBUTED_FILESYSTEM,
        explanation = "Provides operator the ability to delete any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutHDFS extends AbstractHadoopProcessor {

    protected static final String BUFFER_SIZE_KEY = "io.file.buffer.size";
    protected static final int BUFFER_SIZE_DEFAULT = 4096;

    // state

    private Cache<Path, AclStatus> aclCache;

    // relationships

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to HDFS are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to HDFS for some reason are transferred to this relationship")
            .build();

    // properties

    protected static final String REPLACE_RESOLUTION = "replace";
    protected static final String IGNORE_RESOLUTION = "ignore";
    protected static final String FAIL_RESOLUTION = "fail";
    protected static final String APPEND_RESOLUTION = "append";

    protected static final String WRITE_AND_RENAME = "writeAndRename";
    protected static final String SIMPLE_WRITE = "simpleWrite";

    protected static final AllowableValue REPLACE_RESOLUTION_AV = new AllowableValue(REPLACE_RESOLUTION,
            REPLACE_RESOLUTION, "Replaces the existing file if any.");
    protected static final AllowableValue IGNORE_RESOLUTION_AV = new AllowableValue(IGNORE_RESOLUTION, IGNORE_RESOLUTION,
            "Ignores the flow file and routes it to success.");
    protected static final AllowableValue FAIL_RESOLUTION_AV = new AllowableValue(FAIL_RESOLUTION, FAIL_RESOLUTION,
            "Penalizes the flow file and routes it to failure.");
    protected static final AllowableValue APPEND_RESOLUTION_AV = new AllowableValue(APPEND_RESOLUTION, APPEND_RESOLUTION,
            "Appends to the existing file if any, creates a new file otherwise.");

    protected static final AllowableValue WRITE_AND_RENAME_AV = new AllowableValue(WRITE_AND_RENAME, "Write and rename",
            "The processor writes FlowFile data into a temporary file and renames it after completion. This prevents other processes from reading partially written files.");
    protected static final AllowableValue SIMPLE_WRITE_AV = new AllowableValue(SIMPLE_WRITE, "Simple write",
            "The processor writes FlowFile data directly to the destination file. In some cases this might cause reading partially written files.");

    protected static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory")
            .required(true)
            .defaultValue(FAIL_RESOLUTION_AV.getValue())
            .allowableValues(REPLACE_RESOLUTION_AV, IGNORE_RESOLUTION_AV, FAIL_RESOLUTION_AV, APPEND_RESOLUTION_AV)
            .build();

    protected static final PropertyDescriptor WRITING_STRATEGY = new PropertyDescriptor.Builder()
            .name("writing-strategy")
            .displayName("Writing Strategy")
            .description("Defines the approach for writing the FlowFile data.")
            .required(true)
            .defaultValue(WRITE_AND_RENAME_AV.getValue())
            .allowableValues(WRITE_AND_RENAME_AV, SIMPLE_WRITE_AV)
            .build();

    public static final PropertyDescriptor BLOCK_SIZE = new PropertyDescriptor.Builder()
            .name("Block Size")
            .description("Size of each block as written to HDFS. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("IO Buffer Size")
            .description("Amount of memory to use to buffer file contents during IO. This overrides the Hadoop Configuration")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor REPLICATION_FACTOR = new PropertyDescriptor.Builder()
            .name("Replication")
            .description("Number of times that HDFS will replicate each file. This overrides the Hadoop Configuration")
            .addValidator(HadoopValidators.POSITIVE_SHORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor UMASK = new PropertyDescriptor.Builder()
            .name("Permissions umask")
            .description(
                   "A umask represented as an octal number which determines the permissions of files written to HDFS. " +
                           "This overrides the Hadoop property \"fs.permissions.umask-mode\".  " +
                           "If this property and \"fs.permissions.umask-mode\" are undefined, the Hadoop default \"022\" will be used.  "+
                           "If the PutHDFS target folder has a default ACL defined, the umask property is ignored by HDFS.")
            .addValidator(HadoopValidators.UMASK_VALIDATOR)
            .build();

    public static final PropertyDescriptor REMOTE_OWNER = new PropertyDescriptor.Builder()
            .name("Remote Owner")
            .description(
                    "Changes the owner of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change owner")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor REMOTE_GROUP = new PropertyDescriptor.Builder()
            .name("Remote Group")
            .description(
                    "Changes the group of the HDFS file to this value after it is written. This only works if NiFi is running as a user that has HDFS super user privilege to change group")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IGNORE_LOCALITY = new PropertyDescriptor.Builder()
            .name("Ignore Locality")
            .displayName("Ignore Locality")
            .description(
                    "Directs the HDFS system to ignore locality rules so that data is distributed randomly throughout the cluster")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private static final Set<Relationship> relationships;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>(properties);
        props.add(new PropertyDescriptor.Builder()
                .fromPropertyDescriptor(DIRECTORY)
                .description("The parent HDFS directory to which files should be written. The directory will be created if it doesn't exist.")
                .build());
        props.add(CONFLICT_RESOLUTION);
        props.add(WRITING_STRATEGY);
        props.add(BLOCK_SIZE);
        props.add(BUFFER_SIZE);
        props.add(REPLICATION_FACTOR);
        props.add(UMASK);
        props.add(REMOTE_OWNER);
        props.add(REMOTE_GROUP);
        props.add(COMPRESSION_CODEC);
        props.add(IGNORE_LOCALITY);
        return props;
    }

    @Override
    protected void preProcessConfiguration(final Configuration config, final ProcessContext context) {
        // Set umask once, to avoid thread safety issues doing it in onTrigger
        final PropertyValue umaskProp = context.getProperty(UMASK);
        final short dfsUmask;
        if (umaskProp.isSet()) {
            dfsUmask = Short.parseShort(umaskProp.getValue(), 8);
        } else {
            dfsUmask = FsPermission.getUMask(config).toShort();
        }
        FsPermission.setUMask(config, new FsPermission(dfsUmask));
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        aclCache = Caffeine.newBuilder()
                .maximumSize(20L)
                .expireAfterWrite(Duration.ofHours(1))
                .build();
    }

    @OnStopped
    public void onStopped() {
        aclCache.invalidateAll();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final FileSystem hdfs = getFileSystem();
        final Configuration configuration = getConfiguration();
        final UserGroupInformation ugi = getUserGroupInformation();

        if (configuration == null || hdfs == null || ugi == null) {
            getLogger().error("HDFS not configured properly");
            session.transfer(flowFile, getFailureRelationship());
            context.yield();
            return;
        }

        ugi.doAs(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                Path tempDotCopyFile = null;
                FlowFile putFlowFile = flowFile;
                try {
                    final String writingStrategy = context.getProperty(WRITING_STRATEGY).getValue();
                    final Path dirPath = getNormalizedPath(context, DIRECTORY, putFlowFile);
                    checkAclStatus(getAclStatus(dirPath));
                    final String conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();
                    final long blockSize = getBlockSize(context, session, putFlowFile, dirPath);
                    final int bufferSize = getBufferSize(context, session, putFlowFile);
                    final short replication = getReplication(context, session, putFlowFile, dirPath);

                    final CompressionCodec codec = getCompressionCodec(context, configuration);

                    final String filename = codec != null
                            ? putFlowFile.getAttribute(CoreAttributes.FILENAME.key()) + codec.getDefaultExtension()
                            : putFlowFile.getAttribute(CoreAttributes.FILENAME.key());

                    final Path tempCopyFile = new Path(dirPath, "." + filename);
                    final Path copyFile = new Path(dirPath, filename);

                    // Depending on the writing strategy, we might need a temporary file
                    final Path actualCopyFile = (writingStrategy.equals(WRITE_AND_RENAME))
                            ? tempCopyFile
                            : copyFile;

                    // Create destination directory if it does not exist
                    boolean targetDirCreated = false;
                    try {
                        if (!hdfs.getFileStatus(dirPath).isDirectory()) {
                            throw new IOException(dirPath.toString() + " already exists and is not a directory");
                        }
                    } catch (FileNotFoundException fe) {
                        targetDirCreated = hdfs.mkdirs(dirPath);
                        if (!targetDirCreated) {
                            throw new IOException(dirPath.toString() + " could not be created");
                        }
                        changeOwner(context, hdfs, dirPath, flowFile);
                    }

                    final boolean destinationExists = hdfs.exists(copyFile);

                    // If destination file already exists, resolve that based on processor configuration
                    if (destinationExists) {
                        switch (conflictResponse) {
                            case REPLACE_RESOLUTION:
                                if (hdfs.delete(copyFile, false)) {
                                    getLogger().info("deleted {} in order to replace with the contents of {}",
                                            new Object[]{copyFile, putFlowFile});
                                }
                                break;
                            case IGNORE_RESOLUTION:
                                session.transfer(putFlowFile, getSuccessRelationship());
                                getLogger().info("transferring {} to success because file with same name already exists",
                                        new Object[]{putFlowFile});
                                return null;
                            case FAIL_RESOLUTION:
                                session.transfer(session.penalize(putFlowFile), getFailureRelationship());
                                getLogger().warn("penalizing {} and routing to failure because file with same name already exists",
                                        new Object[]{putFlowFile});
                                return null;
                            default:
                                break;
                        }
                    }

                    // Write FlowFile to temp file on HDFS
                    final StopWatch stopWatch = new StopWatch(true);
                    session.read(putFlowFile, new InputStreamCallback() {

                        @Override
                        public void process(InputStream in) throws IOException {
                            OutputStream fos = null;
                            Path createdFile = null;
                            try {
                                if (conflictResponse.equals(APPEND_RESOLUTION) && destinationExists) {
                                    fos = hdfs.append(copyFile, bufferSize);
                                } else {
                                    final EnumSet<CreateFlag> cflags = EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE);

                                    if (shouldIgnoreLocality(context, session)) {
                                        cflags.add(CreateFlag.IGNORE_CLIENT_LOCALITY);
                                    }

                                    fos = hdfs.create(actualCopyFile, FsCreateModes.applyUMask(FsPermission.getFileDefault(),
                                            FsPermission.getUMask(hdfs.getConf())), cflags, bufferSize, replication, blockSize,
                                            null, null);
                                }

                                if (codec != null) {
                                    fos = codec.createOutputStream(fos);
                                }
                                createdFile = actualCopyFile;
                                BufferedInputStream bis = new BufferedInputStream(in);
                                StreamUtils.copy(bis, fos);
                                bis = null;
                                fos.flush();
                            } finally {
                                try {
                                    if (fos != null) {
                                        fos.close();
                                    }
                                } catch (Throwable t) {
                                    // when talking to remote HDFS clusters, we don't notice problems until fos.close()
                                    if (createdFile != null) {
                                        try {
                                            hdfs.delete(createdFile, false);
                                        } catch (Throwable ignore) {
                                        }
                                    }
                                    throw t;
                                }
                                fos = null;
                            }
                        }

                    });
                    stopWatch.stop();
                    final String dataRate = stopWatch.calculateDataRate(putFlowFile.getSize());
                    final long millis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                    tempDotCopyFile = tempCopyFile;

                    if  (
                        writingStrategy.equals(WRITE_AND_RENAME)
                        && (!conflictResponse.equals(APPEND_RESOLUTION) || (conflictResponse.equals(APPEND_RESOLUTION) && !destinationExists))
                    ) {
                        boolean renamed = false;

                        for (int i = 0; i < 10; i++) { // try to rename multiple times.
                            if (hdfs.rename(tempCopyFile, copyFile)) {
                                renamed = true;
                                break;// rename was successful
                            }
                            Thread.sleep(200L);// try waiting to let whatever might cause rename failure to resolve
                        }
                        if (!renamed) {
                            hdfs.delete(tempCopyFile, false);
                            throw new ProcessException("Copied file to HDFS but could not rename dot file " + tempCopyFile
                                    + " to its final filename");
                        }

                        changeOwner(context, hdfs, copyFile, flowFile);
                    }

                    getLogger().info("copied {} to HDFS at {} in {} milliseconds at a rate of {}",
                            new Object[]{putFlowFile, copyFile, millis, dataRate});

                    final String newFilename = copyFile.getName();
                    final String hdfsPath = copyFile.getParent().toString();
                    putFlowFile = session.putAttribute(putFlowFile, CoreAttributes.FILENAME.key(), newFilename);
                    putFlowFile = session.putAttribute(putFlowFile, ABSOLUTE_HDFS_PATH_ATTRIBUTE, hdfsPath);
                    putFlowFile = session.putAttribute(putFlowFile, TARGET_HDFS_DIR_CREATED_ATTRIBUTE, String.valueOf(targetDirCreated));
                    final Path qualifiedPath = copyFile.makeQualified(hdfs.getUri(), hdfs.getWorkingDirectory());
                    session.getProvenanceReporter().send(putFlowFile, qualifiedPath.toString());

                    session.transfer(putFlowFile, getSuccessRelationship());

                } catch (final IOException e) {
                    Optional<GSSException> causeOptional = findCause(e, GSSException.class, gsse -> GSSException.NO_CRED == gsse.getMajor());
                    if (causeOptional.isPresent()) {
                        getLogger().warn("An error occurred while connecting to HDFS. "
                                        + "Rolling back session, and penalizing flow file {}",
                                new Object[] {putFlowFile.getAttribute(CoreAttributes.UUID.key()), causeOptional.get()});
                        session.rollback(true);
                    } else {
                        getLogger().error("Failed to access HDFS due to {}", new Object[]{e});
                        session.transfer(putFlowFile, getFailureRelationship());
                    }
                } catch (final Throwable t) {
                    if (tempDotCopyFile != null) {
                        try {
                            hdfs.delete(tempDotCopyFile, false);
                        } catch (Exception e) {
                            getLogger().error("Unable to remove temporary file {} due to {}", new Object[]{tempDotCopyFile, e});
                        }
                    }
                    getLogger().error("Failed to write to HDFS due to {}", new Object[]{t});
                    session.transfer(session.penalize(putFlowFile), getFailureRelationship());
                    context.yield();
                }

                return null;
            }

            private void checkAclStatus(final AclStatus aclStatus) throws IOException {
                final boolean isDefaultACL = aclStatus.getEntries().stream().anyMatch(
                        aclEntry -> AclEntryScope.DEFAULT.equals(aclEntry.getScope()));
                final boolean isSetUmask = context.getProperty(UMASK).isSet();
                if (isDefaultACL && isSetUmask) {
                    throw new IOException("PutHDFS umask setting is ignored by HDFS when HDFS default ACL is set.");
                }
            }

            private AclStatus getAclStatus(final Path dirPath) {
                return aclCache.get(dirPath, fn -> {
                    try {
                        return hdfs.getAclStatus(dirPath);
                    } catch (IOException e) {
                        throw new UncheckedIOException(String.format("Unable to query ACL for directory [%s]", dirPath), e);
                    }
                });
            }
        });
    }

    protected Relationship getSuccessRelationship() {
        return REL_SUCCESS;
    }

    protected Relationship getFailureRelationship() {
        return REL_FAILURE;
    }

    protected long getBlockSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, Path dirPath) {
        final Double blockSizeProp = context.getProperty(BLOCK_SIZE).asDataSize(DataUnit.B);
        return blockSizeProp != null ? blockSizeProp.longValue() : getFileSystem().getDefaultBlockSize(dirPath);
    }

    protected int getBufferSize(final ProcessContext context, final ProcessSession session, final FlowFile flowFile) {
        final Double bufferSizeProp = context.getProperty(BUFFER_SIZE).asDataSize(DataUnit.B);
        return bufferSizeProp != null ? bufferSizeProp.intValue() : getConfiguration().getInt(BUFFER_SIZE_KEY, BUFFER_SIZE_DEFAULT);
    }

    protected short getReplication(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, Path dirPath) {
        final Integer replicationProp = context.getProperty(REPLICATION_FACTOR).asInteger();
        return replicationProp != null ? replicationProp.shortValue() : getFileSystem()
                .getDefaultReplication(dirPath);
    }

    protected boolean shouldIgnoreLocality(final ProcessContext context, final ProcessSession session) {
        return context.getProperty(IGNORE_LOCALITY).asBoolean();
    }

    protected String getOwner(final ProcessContext context, final FlowFile flowFile) {
        final String owner = context.getProperty(REMOTE_OWNER).evaluateAttributeExpressions(flowFile).getValue();
        return owner == null || owner.isEmpty() ? null : owner;
    }

    protected String getGroup(final ProcessContext context, final FlowFile flowFile) {
        final String group = context.getProperty(REMOTE_GROUP).evaluateAttributeExpressions(flowFile).getValue();
        return group == null || group.isEmpty() ? null : group;
    }

    /**
     * Returns an optional with the first throwable in the causal chain that is assignable to the provided cause type,
     * and satisfies the provided cause predicate, {@link Optional#empty()} otherwise.
     * @param t The throwable to inspect for the cause.
     * @return
     */
    private <T extends Throwable> Optional<T> findCause(Throwable t, Class<T> expectedCauseType, Predicate<T> causePredicate) {
        Stream<Throwable> causalChain = Throwables.getCausalChain(t).stream();
        return causalChain
                .filter(expectedCauseType::isInstance)
                .map(expectedCauseType::cast)
                .filter(causePredicate)
                .findFirst();
    }

    protected void changeOwner(final ProcessContext context, final FileSystem hdfs, final Path name, final FlowFile flowFile) {
        try {
            // Change owner and group of file if configured to do so
            final String owner = getOwner(context, flowFile);
            final String group = getGroup(context, flowFile);

            if (owner != null || group != null) {
                hdfs.setOwner(name, owner, group);
            }
        } catch (Exception e) {
            getLogger().warn("Could not change owner or group of {} on HDFS due to {}", new Object[]{name, e});
        }
    }
}
