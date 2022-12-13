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

package org.apache.nifi.processors.aws.ml.textract;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.AmazonTextractClient;
import com.amazonaws.services.textract.model.StartDocumentAnalysisRequest;
import com.amazonaws.services.textract.model.StartDocumentAnalysisResult;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.StartExpenseAnalysisRequest;
import com.amazonaws.services.textract.model.StartExpenseAnalysisResult;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStarter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Textract"})
@CapabilityDescription("Trigger a AWS Textract job. It should be followed by GetAwsTextractJobStatus processor in order to monitor job status.")
@SeeAlso({GetAwsTextractJobStatus.class})
public class StartAwsTextractJob extends AwsMachineLearningJobStarter<AmazonTextractClient, AmazonWebServiceRequest, AmazonWebServiceResult> {
    private static final String DOCUMENT_ANALYSIS = "Document Analysis";
    private static final String DOCUMENT_TEXT_DETECTION = "Document Text Detection";
    private static final String EXPENSE_ANALYSIS = "Expense Analysis";
    public static final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
            .name("textract-type")
            .displayName("Textract Type")
            .allowableValues(
                    new AllowableValue(DOCUMENT_ANALYSIS, DOCUMENT_ANALYSIS, "Document analysis operation return 5 categories of document extraction " +
                            "— text, forms, tables, query responses, and signatures"),
                    new AllowableValue(DOCUMENT_TEXT_DETECTION, DOCUMENT_TEXT_DETECTION, "Text detection operation that return only the text detected" +
                            " in a document."),
                    new AllowableValue(EXPENSE_ANALYSIS, EXPENSE_ANALYSIS, "Extracts relevant data such as such as vendor and receiver " +
                            "contact information, from almost any invoice or receipt without the need for any templates or configuration"))
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(DOCUMENT_ANALYSIS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final List<PropertyDescriptor> TEXTRACT_PROPERTIES =
        Collections.unmodifiableList(Stream.concat(PROPERTIES.stream(), Stream.of(TYPE)).collect(Collectors.toList()));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return TEXTRACT_PROPERTIES;
    }

    @Override
    protected void postProcessFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile, AmazonWebServiceResult response) {
        super.postProcessFlowFile(context, session, flowFile, response);
        session.putAttribute(flowFile, TYPE.getName(), context.getProperty(TYPE.getName()).getValue());
    }

    @Override
    protected AmazonTextractClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTextractClient) AmazonTextractClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    protected AmazonWebServiceResult sendRequest(AmazonWebServiceRequest request, ProcessContext context) {
        String textractType = context.getProperty(TYPE.getName()).getValue();
        AmazonWebServiceResult result;
        switch (textractType) {
            case DOCUMENT_ANALYSIS :
                result = getClient().startDocumentAnalysis((StartDocumentAnalysisRequest) request);
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = getClient().startDocumentTextDetection((StartDocumentTextDetectionRequest) request);
                break;
            case EXPENSE_ANALYSIS :
                result = getClient().startExpenseAnalysis((StartExpenseAnalysisRequest) request);
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type: " + textractType);
        }
        return result;
    }

    @Override
    protected Class<? extends AmazonWebServiceRequest> getAwsRequestClass(ProcessContext context) {
        String typeOfTextract = context.getProperty(TYPE.getName()).getValue();
        Class<? extends AmazonWebServiceRequest>  result;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS:
                result = StartDocumentAnalysisRequest.class;
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = StartDocumentTextDetectionRequest.class;
                break;
            case EXPENSE_ANALYSIS:
                result = StartExpenseAnalysisRequest.class;
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type.");
        }
        return result;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, AmazonWebServiceResult amazonWebServiceResult) {
        String typeOfTextract = context.getProperty(TYPE.getName()).getValue();
        String  result;
        switch (typeOfTextract) {
            case DOCUMENT_ANALYSIS:
                result = ((StartDocumentAnalysisResult) amazonWebServiceResult).getJobId();
                break;
            case DOCUMENT_TEXT_DETECTION:
                result = ((StartDocumentTextDetectionResult) amazonWebServiceResult).getJobId();
                break;
            case EXPENSE_ANALYSIS:
                result = ((StartExpenseAnalysisResult) amazonWebServiceResult).getJobId();
                break;
            default: throw new UnsupportedOperationException("Unsupported textract type.");
        }
        return result;
    }
}
