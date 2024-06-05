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
package org.apache.nifi.processors.network.pcap;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@SideEffectFree
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"PCAP", "Splitter", "Network", "Packet", "Capture", "Wireshark", "TShark", "TcpDump", "WinDump", "sniffers"})
@CapabilityDescription("Splits a pcap file into multiple pcap files based on a maximum size.")
@WritesAttributes({
    @WritesAttribute(
        attribute = SplitPCAP.ERROR_REASON_LABEL,
        description = "The reason the flowfile was sent to the failure relationship."
    ),
    @WritesAttribute(
        attribute = "fragment.identifier",
        description = "All split PCAP FlowFiles produced from the same parent PCAP FlowFile will have the same randomly generated UUID added for this attribute"
    ),
    @WritesAttribute(
        attribute = "fragment.index",
        description = "A one-up number that indicates the ordering of the split PCAP FlowFiles that were created from a single parent PCAP FlowFile"
    ),
    @WritesAttribute(
        attribute = "fragment.count",
        description = "The number of split PCAP FlowFiles generated from the parent PCAP FlowFile"
    ),
    @WritesAttribute(
        attribute = "segment.original.filename ",
        description = "The filename of the parent PCAP FlowFile"
    )
})

public class SplitPCAP extends AbstractProcessor {

    protected static final String ERROR_REASON_LABEL = "ERROR_REASON";
    protected static String ERROR_REASON_VALUE = "";
    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();

    public static final PropertyDescriptor PCAP_MAX_SIZE = new PropertyDescriptor
            .Builder().name("PCAP Max Size")
            .displayName("PCAP Max Size")
            .description("Maximum size of the output pcap file in bytes. Defaults to 1MB (1000000)")
            .required(true)
            .defaultValue("1MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to "
            + "this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship.")
            .build();
    public static final Relationship REL_SPLIT = new Relationship.Builder()
            .name("split")
            .description("The individual PCAP 'segments' of the original PCAP FlowFile will be routed to this relationship.")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(PCAP_MAX_SIZE);
    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_ORIGINAL, REL_FAILURE, REL_SPLIT);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * This method is called when a trigger event occurs in the processor.
     * It processes the incoming flow file, splits it into smaller pcap files based on the PCAP Max Size,
     * and transfers the split pcap files to the success relationship.
     * If the flow file is empty or not parseable, it is transferred to the failure relationship.
     *
     * @param context  the process context
     * @param session  the process session
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final List<FlowFile> splitFilesList = new ArrayList<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream inStream) {
                try {
                    final int pcapMaxSize = context.getProperty(PCAP_MAX_SIZE.getName()).asDataSize(DataUnit.B).intValue();
                    final List<Packet> loadedPackets = new ArrayList<>();
                    final BufferedInputStream bInStream = new BufferedInputStream(inStream);

                    if ( bInStream.available() == 0 ) {
                        throw new IOException("PCAP file empty.");
                    }

                    final byte[] pcapHeaderArray = new byte[PCAP.PCAP_HEADER_LENGTH];
                    bInStream.read(pcapHeaderArray, 0, PCAP.PCAP_HEADER_LENGTH);
                    int currentPcapTotalLength = PCAP.PCAP_HEADER_LENGTH;

                    final PCAP templatePcap = new PCAP(new ByteBufferInterface(pcapHeaderArray));

                    while (bInStream.available() > 0) {

                        byte[] packetHeaderArray = new byte[Packet.PACKET_HEADER_LENGTH];
                        bInStream.read(packetHeaderArray, 0, Packet.PACKET_HEADER_LENGTH);
                        Packet currentPacket = new Packet(packetHeaderArray, templatePcap);

                        if (currentPacket.totalLength() > pcapMaxSize) {
                            throw new IOException("PCAP contains a packet larger than the max size.");
                        }

                        if (currentPacket.isInvalid()) {
                            throw new IOException("PCAP contains an invalid packet.");
                        }

                        byte[] packetBodyArray = new byte[(int) currentPacket.expectedLength()];

                        bInStream.read(packetBodyArray, 0, (int) currentPacket.expectedLength());
                        currentPacket.setBody(packetBodyArray);

                        if (currentPcapTotalLength + currentPacket.totalLength() > pcapMaxSize) {

                            templatePcap.packets().addAll(loadedPackets);
                            FlowFile newFlowFile = session.create(flowFile);
                            try (final OutputStream out = session.write(newFlowFile)) {
                                out.write(templatePcap.readBytesFull());
                                splitFilesList.add(newFlowFile);

                            } catch (IOException e) {
                                throw new IOException(e.getMessage());
                            }

                            loadedPackets.clear();
                            loadedPackets.add(currentPacket);
                            currentPcapTotalLength = PCAP.PCAP_HEADER_LENGTH;
                            templatePcap.packets().clear();
                        }

                        loadedPackets.add(currentPacket);
                        currentPcapTotalLength += currentPacket.totalLength();
                    }

                    // If there are any packets left over, create a new flowfile.
                    if (!loadedPackets.isEmpty()) {
                        templatePcap.packets().addAll(loadedPackets);
                        FlowFile newFlowFile = session.create(flowFile);
                        try (final OutputStream out = session.write(newFlowFile)) {
                            out.write(templatePcap.readBytesFull());
                            splitFilesList.add(newFlowFile);

                        } catch (IOException e) {
                            throw new IOException(e.getMessage());
                        }
                    }

                } catch (IOException e) {
                    for (FlowFile splitFile : splitFilesList) {
                        session.remove(splitFile);
                    }
                    splitFilesList.clear();
                    SplitPCAP.ERROR_REASON_VALUE = e.getMessage();
                }
            }
        });

        if (splitFilesList.size() == 0) {
            session.putAttribute(flowFile, ERROR_REASON_LABEL, ERROR_REASON_VALUE);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String fragmentId = UUID.randomUUID().toString();
        final String originalFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final String originalFileNameWithoutExtension = originalFileName.substring(0, originalFileName.lastIndexOf("."));

        IntStream.range(0, splitFilesList.size()).forEach(index -> {
            FlowFile split = splitFilesList.get(index);
            Map<String, String> attributes = new HashMap<>();
            attributes.put(CoreAttributes.FILENAME.key(), originalFileNameWithoutExtension + "-" + index + ".pcap");
            attributes.put(FRAGMENT_COUNT, String.valueOf(splitFilesList.size()));
            attributes.put(FRAGMENT_ID, fragmentId);
            attributes.put(FRAGMENT_INDEX, Integer.toString(index));
            attributes.put(SEGMENT_ORIGINAL_FILENAME, originalFileName);
            session.putAllAttributes(split, attributes);
        });
        session.transfer(splitFilesList, REL_SPLIT);
        session.transfer(flowFile, REL_ORIGINAL);
    }
}
