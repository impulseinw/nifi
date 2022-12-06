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
package org.apache.nifi.jasn1.preprocess;

import org.apache.nifi.jasn1.preprocess.preprocessors.ConstraintAsnPreprocessor;
import org.apache.nifi.jasn1.preprocess.preprocessors.HuggingCommentAsnPreprocessor;
import org.apache.nifi.jasn1.preprocess.preprocessors.VersionBracketAsnPreprocessor;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class NiFiASNPreprocessorEngine {
    public static final String COMMA = "\\s*,\\s*";

    private static final List<NiFiASNPreprocessor> PREPROCESSORS = Arrays.asList(
            new HuggingCommentAsnPreprocessor(),
            new VersionBracketAsnPreprocessor(),
            new ConstraintAsnPreprocessor()
    );

    public String preprocess(
            ComponentLog componentLog,
            String asnFilesString,
            String outputDirectory
    ) {
        String[] inputFiles = asnFilesString.split(COMMA);

        StringJoiner preprocessedInputFiles = new StringJoiner(",");

        for (String inputFile : inputFiles) {
            Path inputFilePath = Paths.get(inputFile);
            Path fileName = inputFilePath.getFileName();

            List<String> lines = readAsnLines(componentLog, inputFile, inputFilePath);

            List<String> preprocessedLines = preprocessAsn(lines);

            String preprocessedAsn = preprocessedLines
                    .stream()
                    .collect(Collectors.joining(System.lineSeparator()));

            Path preprocessedAsnPath = Paths.get(outputDirectory, fileName.toString());
            preprocessedInputFiles.add(preprocessedAsnPath.toString());

            writePreprocessedAsn(componentLog, preprocessedAsn, preprocessedAsnPath);
        }

        return preprocessedInputFiles.toString();
    }

    List<String> preprocessAsn(List<String> lines) {
        List<String> preprocessedAsn = lines;

        for (NiFiASNPreprocessor preprocessor : getPreprocessors()) {
            preprocessedAsn = preprocessor.preprocessAsn(preprocessedAsn);
        }

        return preprocessedAsn;
    }

    List<String> readAsnLines(ComponentLog componentLog, String inputFile, Path inputFilePath) {
        List<String> lines;
        try {
            lines = Files.readAllLines(inputFilePath);
        } catch (IOException e) {
            componentLog.error("Couldn't read {}", inputFile, e);
            throw new UncheckedIOException(e);
        }
        return lines;
    }

    void writePreprocessedAsn(ComponentLog componentLog, String preprocessedAsn, Path preprocessedAsnPath) {
        try {
            Files.write(preprocessedAsnPath, preprocessedAsn.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            componentLog.error("Couldn't write {}", preprocessedAsnPath.toString(), e);
            throw new UncheckedIOException(e);
        }
    }

    List<NiFiASNPreprocessor> getPreprocessors() {
        return PREPROCESSORS;
    }
}
