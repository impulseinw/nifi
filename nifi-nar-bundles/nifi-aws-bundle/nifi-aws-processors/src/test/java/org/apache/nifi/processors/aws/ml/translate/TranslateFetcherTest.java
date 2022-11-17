/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml.translate;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.AWS_CREDENTIALS_PROVIDER_SERVICE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_FAILURE;
import static org.apache.nifi.processors.aws.AbstractAWSProcessor.REL_SUCCESS;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusGetter.AWS_TASK_ID_PROPERTY;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusGetter.AWS_TASK_OUTPUT_LOCATION;
import static org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusGetter.REL_RUNNING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.DescribeTextTranslationJobRequest;
import com.amazonaws.services.translate.model.DescribeTextTranslationJobResult;
import com.amazonaws.services.translate.model.JobStatus;
import com.amazonaws.services.translate.model.OutputDataConfig;
import com.amazonaws.services.translate.model.TextTranslationJobProperties;
import java.util.Collections;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TranslateFetcherTest {
    private static final String TEST_TASK_ID = "testTaskId";
    private static final String CONTENT_STRING = "content";
    private static final String AWS_CREDENTIALS_PROVIDER_NAME = "awsCredetialProvider";
    private static final String OUTPUT_LOCATION_PATH = "outputLocation";
    private TestRunner runner = null;
    private AmazonTranslateClient mockTranslateClient = null;
    private AWSCredentialsProviderService mockAwsCredentialsProvider = null;

    @BeforeEach
    public void setUp() throws InitializationException {
        mockTranslateClient = Mockito.mock(AmazonTranslateClient.class);
        mockAwsCredentialsProvider = Mockito.mock(AWSCredentialsProviderService.class);
        when(mockAwsCredentialsProvider.getIdentifier()).thenReturn(AWS_CREDENTIALS_PROVIDER_NAME);
        final GetAwsTranslateJobStatus mockPollyFetcher = new GetAwsTranslateJobStatus() {
            protected AmazonTranslateClient getClient() {
                return mockTranslateClient;
            }

            @Override
            protected AmazonTranslateClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
                return mockTranslateClient;
            }
        };
        runner = TestRunners.newTestRunner(mockPollyFetcher);
        runner.addControllerService(AWS_CREDENTIALS_PROVIDER_NAME, mockAwsCredentialsProvider);
        runner.enableControllerService(mockAwsCredentialsProvider);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, AWS_CREDENTIALS_PROVIDER_NAME);
    }

    @Test
    public void testTranscribeTaskInProgress() {
        ArgumentCaptor<DescribeTextTranslationJobRequest> requestCaptor = ArgumentCaptor.forClass(DescribeTextTranslationJobRequest.class);
        TextTranslationJobProperties task = new TextTranslationJobProperties()
                .withJobId(TEST_TASK_ID)
                .withJobStatus(JobStatus.IN_PROGRESS);
        DescribeTextTranslationJobResult taskResult = new DescribeTextTranslationJobResult().withTextTranslationJobProperties(task);
        when(mockTranslateClient.describeTextTranslationJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_RUNNING);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

    @Test
    public void testTranscribeTaskCompleted() {
        ArgumentCaptor<DescribeTextTranslationJobRequest> requestCaptor = ArgumentCaptor.forClass(DescribeTextTranslationJobRequest.class);
        TextTranslationJobProperties task = new TextTranslationJobProperties()
                .withJobId(TEST_TASK_ID)
                .withOutputDataConfig(new OutputDataConfig().withS3Uri(OUTPUT_LOCATION_PATH))
                .withJobStatus(JobStatus.COMPLETED);
        DescribeTextTranslationJobResult taskResult = new DescribeTextTranslationJobResult().withTextTranslationJobProperties(task);
        when(mockTranslateClient.describeTextTranslationJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertAllFlowFilesContainAttribute(AWS_TASK_OUTPUT_LOCATION);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

    @Test
    public void testTranscribeTaskFailed() {
        ArgumentCaptor<DescribeTextTranslationJobRequest> requestCaptor = ArgumentCaptor.forClass(DescribeTextTranslationJobRequest.class);
        TextTranslationJobProperties task = new TextTranslationJobProperties()
                .withJobId(TEST_TASK_ID)
                .withJobStatus(JobStatus.FAILED);
        DescribeTextTranslationJobResult taskResult = new DescribeTextTranslationJobResult().withTextTranslationJobProperties(task);
        when(mockTranslateClient.describeTextTranslationJob(requestCaptor.capture())).thenReturn(taskResult);
        runner.enqueue(CONTENT_STRING, Collections.singletonMap(AWS_TASK_ID_PROPERTY, TEST_TASK_ID));
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
        assertEquals(requestCaptor.getValue().getJobId(), TEST_TASK_ID);
    }

}