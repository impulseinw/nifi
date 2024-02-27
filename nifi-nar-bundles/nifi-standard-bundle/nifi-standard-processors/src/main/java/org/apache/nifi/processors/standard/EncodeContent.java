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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32InputStream;
import org.apache.commons.codec.binary.Base32OutputStream;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.encoding.EncodingMode;
import org.apache.nifi.processors.standard.encoding.EncodingType;
import org.apache.nifi.processors.standard.encoding.LineOutputMode;
import org.apache.nifi.processors.standard.util.ValidatingBase32InputStream;
import org.apache.nifi.processors.standard.util.ValidatingBase64InputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"encode", "decode", "base64", "base32", "hex"})
@CapabilityDescription("Encode or decode the contents of a FlowFile using Base64, Base32, or hex encoding schemes")
public class EncodeContent extends AbstractProcessor {

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encoded or decoded")
            .required(true)
            .allowableValues(EncodingMode.class)
            .defaultValue(EncodingMode.ENCODE)
            .build();

    public static final PropertyDescriptor ENCODING = new PropertyDescriptor.Builder()
            .name("Encoding")
            .description("Specifies the type of encoding used")
            .required(true)
            .allowableValues(EncodingType.class)
            .defaultValue(EncodingType.BASE64_ENCODING)
            .build();

    static final PropertyDescriptor LINE_OUTPUT_MODE = new PropertyDescriptor.Builder()
            .name("Line Output Mode")
            .displayName("Line Output Mode")
            .description("If set to 'single-line', the encoded FlowFile content will output as a single line. If set to 'multiple-lines', "
                + "it will output as multiple lines. This property is only applicable when Base64 or Base32 encoding is selected.")
            .required(false)
            .defaultValue(LineOutputMode.SINGLE_LINE)
            .allowableValues(LineOutputMode.class)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(MODE, EncodingMode.ENCODE)
            .dependsOn(ENCODING, EncodingType.BASE64_ENCODING, EncodingType.BASE32_ENCODING)
            .build();

    static final PropertyDescriptor ENCODED_LINE_SEPARATOR = new PropertyDescriptor.Builder()
        .name("Encoded Content Line Separator")
        .displayName("Encoded Content Line Separator")
        .description("Each line of encoded data will be terminated with this byte sequence (e.g. \\r\\n"
                + "). This property defaults to the system-dependent line separator string.  If `line-length` <= 0, "
                + "the `line-separator` property is not used. This property is not used for `hex` encoding.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(System.lineSeparator())
        .addValidator(Validator.VALID)
        .dependsOn(MODE, EncodingMode.ENCODE)
        .dependsOn(ENCODING, EncodingType.BASE64_ENCODING, EncodingType.BASE32_ENCODING)
        .build();

    static final PropertyDescriptor ENCODED_LINE_LENGTH = new PropertyDescriptor.Builder()
        .name("Encoded Content Line Length")
        .displayName("Encoded Content Line Length")
        .description("Each line of encoded data will contain `encoded-line-length` characters (rounded down to the nearest multiple of 4). "
            + "If `encoded-line-length` <= 0, the encoded data is not divided into lines. This property is "
            + "ignored if `line-output-mode` is set to `single-line`.")
        .required(false)
        .defaultValue("76")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .dependsOn(MODE, EncodingMode.ENCODE)
        .dependsOn(ENCODING, EncodingType.BASE64_ENCODING, EncodingType.BASE32_ENCODING)
        .dependsOn(LINE_OUTPUT_MODE, LineOutputMode.MULTIPLE_LINES)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully encoded or decoded will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be encoded or decoded will be routed to failure")
            .build();

    private static final int BUFFER_SIZE = 8192;

    private static final List<PropertyDescriptor> properties = List.of(MODE,
        ENCODING, LINE_OUTPUT_MODE, ENCODED_LINE_SEPARATOR, ENCODED_LINE_LENGTH);

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS,
        REL_FAILURE);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final boolean encode = context.getProperty(MODE).getValue().equals(EncodingMode.ENCODE.getValue());
        final EncodingType encoding = EncodingType.valueOf(context.getProperty(ENCODING).getValue());
        final boolean singleLineOutput = context.getProperty(LINE_OUTPUT_MODE).getValue().equals(LineOutputMode.SINGLE_LINE.getValue());
        final int lineLength = context.getProperty(ENCODED_LINE_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();
        final String lineSeparator = context.getProperty(ENCODED_LINE_SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();

        final StreamCallback callback = getStreamCallback(encode, encoding, Boolean.TRUE.equals(singleLineOutput) ? -1 : lineLength, lineSeparator);

        try {
            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, callback);

            getLogger().info("{} completed {}", encode ? "Encoding" : "Decoding", flowFile);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("{} failed {}", encode ? "Encoding" : "Decoding", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static StreamCallback getStreamCallback(final Boolean encode, final EncodingType encoding,
        final int lineLength, final String lineSeparator) {
        switch(encoding) {
            case BASE64_ENCODING:
                return encode ? new EncodeBase64(lineLength, lineSeparator) : new DecodeBase64();
            case BASE32_ENCODING:
                return encode ? new EncodeBase32(lineLength, lineSeparator) : new DecodeBase32();
            default:
                return encode ? new EncodeHex() : new DecodeHex();
        }
    }

    private static class EncodeBase64 implements StreamCallback {

        private int lineLength;
        private String lineSeparator;

        public EncodeBase64(final int lineLength,
            final String lineSeparator) {
            this.lineLength = lineLength;
            this.lineSeparator = lineSeparator;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base64OutputStream bos = new Base64OutputStream(out,
                true,
                this.lineLength,
                this.lineSeparator.getBytes())) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private static class DecodeBase64 implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base64InputStream bis = new Base64InputStream(new ValidatingBase64InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private static class EncodeBase32 implements StreamCallback {

        private int lineLength;
        private String lineSeparator;

        public EncodeBase32(final int lineLength,
            final String lineSeparator) {

            this.lineLength = lineLength;
            this.lineSeparator = lineSeparator;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base32OutputStream bos = new Base32OutputStream(out,
                true,
                this.lineLength,
                this.lineSeparator.getBytes())) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private static class DecodeBase32 implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base32InputStream bis = new Base32InputStream(new ValidatingBase32InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private static class EncodeHex implements StreamCallback {

        private static final byte[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[8192];
            byte[] outBuf = new byte[inBuf.length * 2];
            while ((len = in.read(inBuf)) > 0) {
                for (int i = 0; i < len; i++) {
                    outBuf[i * 2] = HEX_CHARS[(inBuf[i] & 0xF0) >>> 4];
                    outBuf[i * 2 + 1] = HEX_CHARS[inBuf[i] & 0x0F];
                }
                out.write(outBuf, 0, len * 2);
            }
            out.flush();
        }
    }

    private static class DecodeHex implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[BUFFER_SIZE];
            Hex h = new Hex();
            while ((len = in.read(inBuf)) > 0) {
                // If the input buffer is of odd length, try to get another byte
                if (len % 2 != 0) {
                    int b = in.read();
                    if (b != -1) {
                        inBuf[len] = (byte) b;
                        len++;
                    }
                }

                // Construct a new buffer bounded to len
                byte[] slice = Arrays.copyOfRange(inBuf, 0, len);
                try {
                    out.write(h.decode(slice));
                } catch (final DecoderException e) {
                    throw new IOException("Hexadecimal decoding failed", e);
                }
            }
            out.flush();
        }
    }
}
