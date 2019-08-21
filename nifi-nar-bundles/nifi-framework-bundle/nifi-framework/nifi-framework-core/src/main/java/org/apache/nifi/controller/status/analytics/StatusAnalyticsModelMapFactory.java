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
package org.apache.nifi.controller.status.analytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;

public class StatusAnalyticsModelMapFactory {

    private final static String QUEUED_COUNT_METRIC = "queuedCount";
    private final static String QUEUED_BYTES_METRIC = "queuedBytes";
    private final static String INPUT_COUNT_METRIC = "inputCount";
    private final static String INPUT_BYTES_METRIC = "inputBytes";
    private final static String OUTPUT_COUNT_METRIC = "outputCount";
    private final static String OUTPUT_BYTES_METRIC = "outputBytes";


    public static Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> getConnectionStatusModelMap(ExtensionManager extensionManager, NiFiProperties niFiProperties){
            Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap = new HashMap<>();
            StatusMetricExtractFunction extract = getConnectionStatusExtractFunction();
            Tuple<StatusAnalyticsModel, StatusMetricExtractFunction> countModelFunction = new Tuple<>(createModelInstance(extensionManager, niFiProperties), extract);
            Tuple<StatusAnalyticsModel, StatusMetricExtractFunction> byteModelFunction = new Tuple<>(createModelInstance(extensionManager, niFiProperties), extract);
            modelMap.put(QUEUED_COUNT_METRIC, countModelFunction);
            modelMap.put(QUEUED_BYTES_METRIC, byteModelFunction);
            return modelMap;
    }

    private static StatusAnalyticsModel createModelInstance(ExtensionManager extensionManager, NiFiProperties nifiProperties) {
        final String implementationClassName = nifiProperties.getProperty(NiFiProperties.ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION, NiFiProperties.DEFAULT_ANALYTICS_CONNECTION_MODEL);
        if (implementationClassName == null) {
            throw new RuntimeException("Cannot create Analytics Model because the NiFi Properties is missing the following property: "
                    + NiFiProperties.ANALYTICS_CONNECTION_MODEL_IMPLEMENTATION);
        }
        try {
            return NarThreadContextClassLoader.createInstance(extensionManager, implementationClassName, StatusAnalyticsModel.class, nifiProperties);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static StatusMetricExtractFunction getConnectionStatusExtractFunction() {

        return (metric, statusHistory) -> {

            List<Double> values = new ArrayList<>();
            List<Double[]> features = new ArrayList<>();
            Random rand = new Random();
            StatusHistoryDTO statusHistoryDTO = StatusHistoryUtil.createStatusHistoryDTO(statusHistory);

            for (StatusSnapshotDTO snap : statusHistoryDTO.getAggregateSnapshots()) {
                List<Double> featureArray = new ArrayList<>();
                Long snapValue = snap.getStatusMetrics().get(metric);
                long snapTime = snap.getTimestamp().getTime();

                featureArray.add((double) snapTime);
                Double randomError = +(rand.nextInt(1000) * .0000001);
                if (metric.equals(QUEUED_COUNT_METRIC)) {

                    Long inputCount = snap.getStatusMetrics().get(INPUT_COUNT_METRIC);
                    Long outputCount = snap.getStatusMetrics().get(OUTPUT_COUNT_METRIC);
                    Double inOutRatio = ((double) outputCount / (double) inputCount) + randomError;
                    featureArray.add(Double.isNaN(inOutRatio) ? randomError : inOutRatio);

                } else {
                    Long inputBytes = snap.getStatusMetrics().get(INPUT_BYTES_METRIC);
                    Long outputBytes = snap.getStatusMetrics().get(OUTPUT_BYTES_METRIC);
                    Double inOutRatio = ((double) outputBytes / (double) inputBytes) + randomError;
                    featureArray.add(Double.isNaN(inOutRatio) ? randomError : inOutRatio);
                }

                values.add((double) snapValue);
                features.add(featureArray.toArray(new Double[featureArray.size()]));

            }
            return new Tuple<>(features.stream(), values.stream());

        };
    }


}
