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
package org.apache.nifi.processors.adx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.KustoOperationResult;
import com.microsoft.azure.kusto.data.KustoResultSetTable;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.adx.AdxConnectionService;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionClientException;
import com.microsoft.azure.kusto.ingest.exceptions.IngestionServiceException;
import com.microsoft.azure.kusto.ingest.result.IngestionResult;
import com.microsoft.azure.kusto.ingest.result.IngestionStatus;
import com.microsoft.azure.kusto.ingest.result.OperationStatus;
import com.microsoft.azure.kusto.ingest.source.StreamSourceInfo;
import com.microsoft.azure.storage.StorageException;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.NodeTypeProvider;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.adx.enums.AzureAdxIngestProcessorParamsEnum;
import org.apache.nifi.processors.adx.enums.DataFormatEnum;
import org.apache.nifi.processors.adx.enums.IngestionIgnoreFirstRecordEnum;
import org.apache.nifi.processors.adx.enums.IngestionReportLevelEnum;
import org.apache.nifi.processors.adx.enums.IngestionReportMethodEnum;
import org.apache.nifi.processors.adx.enums.IngestionStatusEnum;
import org.apache.nifi.processors.adx.enums.RelationshipStatusEnum;
import org.apache.nifi.processors.adx.enums.TransactionalIngestionEnum;
import org.apache.nifi.processors.adx.model.IngestionBatchingPolicy;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

@Tags({"azure", "adx", "microsoft", "data", "explorer"})
@CapabilityDescription("The Azure ADX Processor acts as a ADX sink connector which sends flowFiles using the ADX-Service to the provided Azure Data" +
        "Explorer Ingest Endpoint. The data can be sent through queued ingestion or streaming ingestion to the Azure Data Explorer cluster." +
        "The data ingested to ADX can be in non-transactional mode or transactional mode. " +
        "This processor supports transactionality of the ingested data ie. it ensures no duplicates are inserted while retries during ingestion failures. " +
        "But a word of caution while selecting transactional mode is, it significantly reduces the ingestion time " +
        "since the processor first tries to ingest the data into temporary table before ingesting to the main table. ")
@ReadsAttributes({
        @ReadsAttribute(attribute = "DB_NAME", description = "Specifies the name of the ADX database where the data needs to be stored."),
        @ReadsAttribute(attribute = "TABLE_NAME", description = "Specifies the name of the ADX table where the data needs to be stored."),
        @ReadsAttribute(attribute = "MAPPING_NAME", description = "Specifies the name of the mapping responsible for storing the data in appropriate columns."),
        @ReadsAttribute(attribute = "FLUSH_IMMEDIATE", description = "In case of queued ingestion, this property determines whether the data should be flushed immediately to the ingest endpoint."),
        @ReadsAttribute(attribute = "DATA_FORMAT", description = "Specifies the format of data that is send to Azure Data Explorer."),
        @ReadsAttribute(attribute = "IR_LEVEL", description = "ADX can report events on several levels. Ex- None, Failure and Failure & Success."),
        @ReadsAttribute(attribute = "IR_METHOD", description = "ADX can report events on several methods. Ex- Table, Queue, Table&Queue."),
        @ReadsAttribute(attribute = "IS_TRANSACTIONAL", description = "Default : No ,Incase of any failure, whether we want all our data ingested or none. " +
                "If set to Yes, it increases the data ingestion time significantly because inorder to maintain transactional behaviour, " +
                "the processor first tries to ingest into temporary tables before ingesting into actual table."),
        @ReadsAttribute(attribute = "IGNORE_FIRST_RECORD", description = "Specifies whether we want to ignore ingestion of first record. " +
                "This is primarily applicable for csv files. Default is set to NO"),
        @ReadsAttribute(attribute = "MAX_BATCHING_TIME_SPAN", description = "Applicable only for queued ingestion, specifies the maximum batching timespan. Default = 5 min"),
        @ReadsAttribute(attribute = "MAX_BATCHING_NO_OF_ITEMS", description = "Applicable only for queued ingestion, specifies the maximum number of items/records. Default = 1000 items/records"),
        @ReadsAttribute(attribute = "MAX_BATCHING_RAW_DATA_SIZE_IN_MB", description = "Applicable only for queued ingestion, specifies the maximum size of uncompressed data. Default = 1000MB"),
        @ReadsAttribute(attribute = "TEMP_TABLE_NAME", description = "Applicable only when the IS_TRANSACTIONAL attribute is set to YES. " +
                "This attribute specifies if the user wants to assign custom name to temp table. If not specified, the processor appends _tmp to the actual table name."),
        @ReadsAttribute(attribute = "WAIT_FOR_STATUS", description = "When ingesting data to ADX, this property specifies whether the user wants to wait till we receive the status of ingestion.")
})
@Stateful(scopes = Scope.CLUSTER,description = "Incase the user wants transactionality during data ingestion, " +
        "AzureIngestProcessor uses temporary tables to attempt ingestion initially and to store the ingestion status into temp tables of various nodes, it uses nifi statemanager")
public class AzureAdxIngestProcessor extends AbstractProcessor {

    public static final String FETCH_TABLE_COMMAND = "%s | count";
    public static final String STREAMING_POLICY_SHOW_COMMAND = ".show %s %s policy streamingingestion";
    public static final String DATABASE = "database";

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private AdxConnectionService service;
    private IngestClient ingestClient;
    private Client executionClient;

    public static final AllowableValue AVRO = new AllowableValue(
            DataFormatEnum.AVRO.name(), DataFormatEnum.AVRO.getExtension(),
            DataFormatEnum.AVRO.getDescription());

    public static final AllowableValue APACHEAVRO = new AllowableValue(
            DataFormatEnum.APACHEAVRO.name(), DataFormatEnum.APACHEAVRO.getExtension(),
            DataFormatEnum.APACHEAVRO.getDescription());

    public static final AllowableValue CSV = new AllowableValue(
            DataFormatEnum.CSV.name(), DataFormatEnum.CSV.getExtension(),
            DataFormatEnum.CSV.getDescription());

    public static final AllowableValue JSON = new AllowableValue(
            DataFormatEnum.JSON.name(), DataFormatEnum.JSON.getExtension(),
            DataFormatEnum.JSON.getDescription());

    public static final AllowableValue MULTIJSON = new AllowableValue(
            DataFormatEnum.MULTIJSON.name(), DataFormatEnum.MULTIJSON.getExtension(),
            DataFormatEnum.MULTIJSON.getDescription());

    public static final AllowableValue ORC = new AllowableValue(
            DataFormatEnum.ORC.name(), DataFormatEnum.ORC.getExtension(), DataFormatEnum.ORC.getDescription());

    public static final AllowableValue PARQUET = new AllowableValue(
            DataFormatEnum.PARQUET.name(), DataFormatEnum.PARQUET.getExtension(), DataFormatEnum.PARQUET.getDescription());

    public static final AllowableValue PSV = new AllowableValue(
            DataFormatEnum.PSV.name(), DataFormatEnum.PSV.getExtension(), DataFormatEnum.PSV.getDescription());

    public static final AllowableValue SCSV = new AllowableValue(
            DataFormatEnum.SCSV.name(), DataFormatEnum.SCSV.getExtension(), DataFormatEnum.SCSV.getDescription());

    public static final AllowableValue SOHSV = new AllowableValue(
            DataFormatEnum.SOHSV.name(), DataFormatEnum.SOHSV.getExtension(),
            DataFormatEnum.SOHSV.getDescription());

    public static final AllowableValue TSV = new AllowableValue(
            DataFormatEnum.TSV.name(), DataFormatEnum.TSV.getExtension(), DataFormatEnum.TSV.getDescription());

    public static final AllowableValue TSVE = new AllowableValue(
            DataFormatEnum.TSVE.name(), DataFormatEnum.TSVE.getExtension(),
            DataFormatEnum.TSVE.getDescription());

    public static final AllowableValue TXT = new AllowableValue(
            DataFormatEnum.TXT.name(), DataFormatEnum.TXT.getExtension(),
            DataFormatEnum.TXT.getDescription());

    public static final AllowableValue IRL_NONE = new AllowableValue(
            IngestionReportLevelEnum.IRL_NONE.name(), IngestionReportLevelEnum.IRL_NONE.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_NONE.getDescription());

    public static final AllowableValue IRL_FAIL = new AllowableValue(
            IngestionReportLevelEnum.IRL_FAIL.name(), IngestionReportLevelEnum.IRL_FAIL.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_FAIL.getDescription());

    public static final AllowableValue IRL_FAS = new AllowableValue(
            IngestionReportLevelEnum.IRL_FAS.name(), IngestionReportLevelEnum.IRL_FAS.getIngestionReportLevel(),
            IngestionReportLevelEnum.IRL_FAS.getDescription());

    public static final AllowableValue IRM_QUEUE = new AllowableValue(
            IngestionReportMethodEnum.IRM_QUEUE.name(), IngestionReportMethodEnum.IRM_QUEUE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_QUEUE.getDescription());

    public static final AllowableValue IRM_TABLE = new AllowableValue(
            IngestionReportMethodEnum.IRM_TABLE.name(), IngestionReportMethodEnum.IRM_TABLE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_TABLE.getDescription());

    public static final AllowableValue IRM_TABLEANDQUEUE = new AllowableValue(
            IngestionReportMethodEnum.IRM_TABLEANDQUEUE.name(), IngestionReportMethodEnum.IRM_TABLEANDQUEUE.getIngestionReportMethod(),
            IngestionReportMethodEnum.IRM_TABLEANDQUEUE.getDescription());

    public static final AllowableValue ST_SUCCESS = new AllowableValue(
            IngestionStatusEnum.ST_SUCCESS.name(), IngestionStatusEnum.ST_SUCCESS.getIngestionStatus(),
            IngestionStatusEnum.ST_SUCCESS.getDescription());

    public static final AllowableValue ST_FIREANDFORGET = new AllowableValue(
            IngestionStatusEnum.ST_FIREANDFORGET.name(), IngestionStatusEnum.ST_FIREANDFORGET.getIngestionStatus(),
            IngestionStatusEnum.ST_FIREANDFORGET.getDescription());

    public static final AllowableValue TRANSACTIONAL_YES = new AllowableValue(
            TransactionalIngestionEnum.YES.name(), TransactionalIngestionEnum.YES.getTransactionalIngestion(),
            TransactionalIngestionEnum.YES.getDescription());

    public static final AllowableValue TRANSACTIONAL_NO = new AllowableValue(
            TransactionalIngestionEnum.NO.name(), TransactionalIngestionEnum.NO.getTransactionalIngestion(),
            TransactionalIngestionEnum.NO.getDescription());

    public static final AllowableValue IGNORE_FIRST_RECORD_YES = new AllowableValue(
            IngestionIgnoreFirstRecordEnum.YES.name(), IngestionIgnoreFirstRecordEnum.YES.getIngestFirstRecord(),
            TransactionalIngestionEnum.YES.getDescription());

    public static final AllowableValue IGNORE_FIRST_RECORD_NO = new AllowableValue(
            IngestionIgnoreFirstRecordEnum.NO.name(), IngestionIgnoreFirstRecordEnum.NO.getIngestFirstRecord(),
            IngestionIgnoreFirstRecordEnum.NO.getDescription());

    public static final PropertyDescriptor DB_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.DB_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.DB_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.DB_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.TABLE_NAME.getParamDescription())
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAPPING_NAME = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.MAPPING_NAME.getParamDescription())
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor ADX_SERVICE = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.ADX_SERVICE.getParamDescription())
            .required(true)
            .identifiesControllerService(AdxConnectionService.class)
            .build();
    public static final PropertyDescriptor WAIT_FOR_STATUS = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.WAIT_FOR_STATUS.getParamDescription())
            .required(true)
            .allowableValues(ST_SUCCESS, ST_FIREANDFORGET)
            .defaultValue(ST_SUCCESS.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IS_TRANSACTIONAL = new PropertyDescriptor
            .Builder().name(AzureAdxIngestProcessorParamsEnum.IS_TRANSACTIONAL.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IS_TRANSACTIONAL.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IS_TRANSACTIONAL.getParamDescription())
            .required(false)
            .allowableValues(TRANSACTIONAL_YES, TRANSACTIONAL_NO)
            .defaultValue(TRANSACTIONAL_NO.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TEMP_TABLE_NAME = new PropertyDescriptor
            .Builder().name("TEMP_TABLE_NAME")
            .displayName("Temporary Table Name")
            .description("This property specifies the temporary table name when data ingestion is selected in transactional mode")
            .dependsOn(IS_TRANSACTIONAL,TRANSACTIONAL_YES)
            .required(false)
            .build();

    public static final PropertyDescriptor TEMP_TABLE_SOFT_DELETE_RETENTION = new PropertyDescriptor
            .Builder().name("TEMP_TABLE_SOFT_DELETE_RETENTION")
            .displayName("Temporary table soft delete retention period")
            .description("This property specifies the soft delete retention period of temporary table when data ingestion is selected in transactional mode")
            .dependsOn(IS_TRANSACTIONAL,TRANSACTIONAL_YES)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("1d")
            .build();
    public static final Relationship RL_SUCCEEDED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_SUCCEEDED.name())
            .description(RelationshipStatusEnum.RL_SUCCEEDED.getDescription())
            .build();
    public static final Relationship RL_FAILED = new Relationship.Builder()
            .name(RelationshipStatusEnum.RL_FAILED.name())
            .description(RelationshipStatusEnum.RL_FAILED.getDescription())
            .build();
    static final PropertyDescriptor FLUSH_IMMEDIATE = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.FLUSH_IMMEDIATE.getParamDescription())
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.DATA_FORMAT.getParamDescription())
            .required(true)
            .allowableValues(AVRO, APACHEAVRO, CSV, JSON, MULTIJSON, ORC, PARQUET, PSV, SCSV, SOHSV, TSV, TSVE, TXT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor IR_LEVEL = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IR_LEVEL.getParamDescription())
            .required(true)
            .allowableValues(IRL_NONE, IRL_FAIL, IRL_FAS)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor IR_METHOD = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IR_METHOD.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IR_METHOD.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IR_METHOD.getParamDescription())
            .required(true)
            .allowableValues(IRM_TABLE, IRM_QUEUE, IRM_TABLEANDQUEUE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor IGNORE_FIRST_RECORD = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.IS_IGNORE_FIRST_RECORD.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.IS_IGNORE_FIRST_RECORD.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.IS_IGNORE_FIRST_RECORD.getParamDescription())
            .required(false)
            .allowableValues(IGNORE_FIRST_RECORD_YES, IGNORE_FIRST_RECORD_NO)
            .defaultValue(IGNORE_FIRST_RECORD_NO.getValue())
            .build();

    static final PropertyDescriptor MAX_BATCHING_TIME_SPAN = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_TIME_SPAN.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_TIME_SPAN.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_TIME_SPAN.getParamDescription())
            .required(false)
            .defaultValue("00:05:00")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^(?:(?:([01]?\\d|2[0-3]):)?([0-5]?\\d):)?([0-5]?\\d)$")))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor MAX_BATCHING_NO_OF_ITEMS = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_NO_OF_ITEMS.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_NO_OF_ITEMS.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_NO_OF_ITEMS.getParamDescription())
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(String.valueOf(1000))
            .build();

    static final PropertyDescriptor MAX_BATCHING_RAW_DATA_SIZE_IN_MB = new PropertyDescriptor.Builder()
            .name(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_RAW_DATA_SIZE_IN_MB.name())
            .displayName(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_RAW_DATA_SIZE_IN_MB.getParamDisplayName())
            .description(AzureAdxIngestProcessorParamsEnum.MAX_BATCHING_RAW_DATA_SIZE_IN_MB.getParamDescription())
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue(String.valueOf(1024))
            .build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ADX_SERVICE);
        descriptors.add(DB_NAME);
        descriptors.add(TABLE_NAME);
        descriptors.add(MAPPING_NAME);
        descriptors.add(FLUSH_IMMEDIATE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(IR_LEVEL);
        descriptors.add(IR_METHOD);
        descriptors.add(WAIT_FOR_STATUS);
        descriptors.add(IS_TRANSACTIONAL);
        descriptors.add(IGNORE_FIRST_RECORD);
        descriptors.add(TEMP_TABLE_NAME);
        descriptors.add(TEMP_TABLE_SOFT_DELETE_RETENTION);
        descriptors.add(MAX_BATCHING_NO_OF_ITEMS);
        descriptors.add(MAX_BATCHING_RAW_DATA_SIZE_IN_MB);
        descriptors.add(MAX_BATCHING_TIME_SPAN);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RL_SUCCEEDED);
        relationships.add(RL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = context.getProperty(ADX_SERVICE).asControllerService(AdxConnectionService.class);
        ingestClient = service.getAdxClient();
        executionClient = service.getKustoExecutionClient();

        if (!isIngestorRole(context.getProperty(DB_NAME).getValue(), context.getProperty(TABLE_NAME).getValue(), executionClient)) {
            getLogger().error("User might not have ingestor privileges, table validation will be skipped for all table mappings.");
            throw new ProcessException("User might not have ingestor privileges, table validation will be skipped for all table mappings. ");
        }
        if (service.isStreamingEnabled()) {
            try {
                isStreamingPolicyEnabled(DATABASE, context.getProperty(DB_NAME).getValue(), executionClient, context.getProperty(DB_NAME).getValue());
            } catch (DataClientException | DataServiceException e) {
                throw new ProcessException("Streaming policy is not enabled ");
            }
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            context.yield();
            return;
        }

        IngestionProperties ingestionProperties = new IngestionProperties(context.getProperty(DB_NAME).getValue(),
                context.getProperty(TABLE_NAME).getValue());

        IngestionMapping.IngestionMappingKind ingestionMappingKind = null;

        switch (DataFormatEnum.valueOf(context.getProperty(DATA_FORMAT).getValue())) {
            case AVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.AVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.AVRO.getIngestionMappingKind();
                break;
            case APACHEAVRO:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.APACHEAVRO);
                ingestionMappingKind = IngestionProperties.DataFormat.APACHEAVRO.getIngestionMappingKind();
                break;
            case CSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.CSV);
                ingestionMappingKind = IngestionProperties.DataFormat.CSV.getIngestionMappingKind();
                break;
            case JSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.JSON);
                ingestionMappingKind = IngestionProperties.DataFormat.JSON.getIngestionMappingKind();
                break;
            case MULTIJSON:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.MULTIJSON);
                ingestionMappingKind = IngestionProperties.DataFormat.MULTIJSON.getIngestionMappingKind();
                break;
            case ORC:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.ORC);
                ingestionMappingKind = IngestionProperties.DataFormat.ORC.getIngestionMappingKind();
                break;
            case PARQUET:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PARQUET);
                ingestionMappingKind = IngestionProperties.DataFormat.PARQUET.getIngestionMappingKind();
                break;
            case PSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.PSV);
                ingestionMappingKind = IngestionProperties.DataFormat.PSV.getIngestionMappingKind();
                break;
            case SCSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SCSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SCSV.getIngestionMappingKind();
                break;
            case SOHSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.SOHSV);
                ingestionMappingKind = IngestionProperties.DataFormat.SOHSV.getIngestionMappingKind();
                break;
            case TSV:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSV);
                ingestionMappingKind = IngestionProperties.DataFormat.TSV.getIngestionMappingKind();
                break;
            case TSVE:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TSVE);
                ingestionMappingKind = IngestionProperties.DataFormat.TSVE.getIngestionMappingKind();
                break;
            case TXT:
                ingestionProperties.setDataFormat(IngestionProperties.DataFormat.TXT);
                ingestionMappingKind = IngestionProperties.DataFormat.TXT.getIngestionMappingKind();
                break;
        }

        if (StringUtils.isNotEmpty(context.getProperty(MAPPING_NAME).getValue()) && ingestionMappingKind != null) {
            ingestionProperties.setIngestionMapping(context.getProperty(MAPPING_NAME).getValue(), ingestionMappingKind);
        }

        switch (IngestionReportLevelEnum.valueOf(context.getProperty(IR_LEVEL).getValue())) {
            case IRL_NONE:
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.NONE);
                break;
            case IRL_FAIL:
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_ONLY);
                break;
            case IRL_FAS:
                ingestionProperties.setReportLevel(IngestionProperties.IngestionReportLevel.FAILURES_AND_SUCCESSES);
                break;
        }

        switch (IngestionReportMethodEnum.valueOf(context.getProperty(IR_METHOD).getValue())) {
            case IRM_TABLE:
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.TABLE);
                break;
            case IRM_QUEUE:
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE);
                break;
            case IRM_TABLEANDQUEUE:
                ingestionProperties.setReportMethod(IngestionProperties.IngestionReportMethod.QUEUE_AND_TABLE);
                break;
        }

        if (StringUtils.equalsIgnoreCase(context.getProperty(FLUSH_IMMEDIATE).getValue(), "true")) {
            ingestionProperties.setFlushImmediately(true);
        } else {
            ingestionProperties.setFlushImmediately(false);
        }

        if (StringUtils.equalsIgnoreCase(context.getProperty(IGNORE_FIRST_RECORD).getValue(), IGNORE_FIRST_RECORD_YES.getValue())) {
            ingestionProperties.setIgnoreFirstRecord(true);
        } else {
            ingestionProperties.setIgnoreFirstRecord(false);
        }

        String ingestionBatchingString = addModifyBatchingPolicy(ingestionProperties, context);

        boolean isSingleNodeTempTableIngestionSucceeded = false;
        boolean isClusteredTempTableIngestionSucceeded = false;
        boolean isError = false;

        IngestionProperties ingestionPropertiesCreateTempTable;

        if (StringUtils.equalsIgnoreCase(context.getProperty(IS_TRANSACTIONAL).getValue(), TRANSACTIONAL_YES.getValue()) && !service.isStreamingEnabled()) {
            String tempTableName;
            if (StringUtils.isNotEmpty(context.getProperty(TEMP_TABLE_NAME).getValue())) {
                tempTableName = context.getProperty(TEMP_TABLE_NAME).getValue();
            } else {
                tempTableName = context.getProperty(TABLE_NAME).getValue() + "_tmp";
            }

            ingestionPropertiesCreateTempTable = new IngestionProperties(context.getProperty(DB_NAME).getValue(), tempTableName);
            ingestionPropertiesCreateTempTable.setDataFormat(ingestionProperties.getDataFormat());
            ingestionPropertiesCreateTempTable.setIngestionMapping(ingestionProperties.getIngestionMapping());
            ingestionPropertiesCreateTempTable.setReportLevel(ingestionProperties.getReportLevel());
            ingestionPropertiesCreateTempTable.setReportMethod(ingestionProperties.getReportMethod());
            ingestionPropertiesCreateTempTable.setIgnoreFirstRecord(ingestionProperties.isIgnoreFirstRecord());
            ingestionPropertiesCreateTempTable.setFlushImmediately(ingestionProperties.getFlushImmediately());

            try (final InputStream in = session.read(flowFile)) {
                //check if it is transactional
                StreamSourceInfo info = new StreamSourceInfo(in);
                Map<String, String> stateMap = null;
                //if clustered - update in statemap status as inprogress for that nodeId
                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    StateManager stateManager = context.getStateManager();
                    if (stateManager.getState(Scope.CLUSTER).toMap().isEmpty()) {
                        //getLogger().error(getNodeTypeProvider().getCurrentNode().toString() + "  cluster map is empty");
                        stateMap = new ConcurrentHashMap<>();
                        stateMap.put(getNodeTypeProvider().getCurrentNode().toString(), "IN_PROGRESS");
                        stateManager.setState(stateMap, Scope.CLUSTER);
                        //getLogger().error(getNodeTypeProvider().getCurrentNode().toString() + "  updated cluster map status " + context.getStateManager().getState(Scope.CLUSTER).toMap());
                    } else {
                        //getLogger().error(getNodeTypeProvider().getCurrentNode().toString() + "  some key exist in statemap " + context.getStateManager().getState(Scope.CLUSTER).toMap());
                        Map<String, String> existingMap = stateManager.getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "IN_PROGRESS");
                        stateManager.setState(updatedMap, Scope.CLUSTER);
                        //getLogger().error(getNodeTypeProvider().getCurrentNode().toString() + "  updated cluster map status " + context.getStateManager().getState(Scope.CLUSTER).toMap());
                    }
                    getLogger().info("StateMap  - {}", stateManager.getState(Scope.CLUSTER).toMap());
                    //getLogger().error("node provider values  - {}", getNodeTypeProvider().getClusterMembers());
                }

                //then start creating temp tables
                String columnsAsSchema = showOriginalTableRetentionPolicy(ingestionProperties);

                Calendar calendar = Calendar.getInstance();
                calendar.add(Calendar.DATE, 1);
                String expiryDate = new SimpleDateFormat("yyyy-MM-dd").format(calendar.getTime());

                //drop temp table if exists
                dropTempTableIfExists(ingestionPropertiesCreateTempTable);

                //create temp table
                createTempTable(ingestionPropertiesCreateTempTable,ingestionProperties, columnsAsSchema);

                //alter retention policy of temp table
                alterTempTableRetentionPolicy(ingestionPropertiesCreateTempTable,context);

                //alter auto delete policy of temp table
                alterTempTableAutoDeletePolicy(ingestionPropertiesCreateTempTable,expiryDate);

                //apply batching policy of main table to temporary table
                applyTempTableBatchingPolicy(ingestionPropertiesCreateTempTable, ingestionBatchingString);

                //ingest data
                IngestionResult resultFromTempTable = ingestClient.ingestFromStream(info, ingestionPropertiesCreateTempTable);
                List<IngestionStatus> statuses;
                CompletableFuture<List<IngestionStatus>> future = new CompletableFuture<>();
                ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

                Runnable task = () -> {
                    try {
                        List<IngestionStatus> statuses1 = resultFromTempTable.getIngestionStatusCollection();
                        if (statuses1.get(0).status == OperationStatus.Succeeded
                                || statuses1.get(0).status == OperationStatus.Failed
                                || statuses1.get(0).status == OperationStatus.PartiallySucceeded) {
                            future.complete(statuses1);
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(new ProcessException("Error occurred while checking ingestion status of temp table", e));
                    }
                };

                scheduler.scheduleWithFixedDelay(task, 1, 2, TimeUnit.SECONDS);
                statuses = future.get(1800, TimeUnit.SECONDS);

                shutDownScheduler(scheduler);

                if (statuses.get(0).status == OperationStatus.Succeeded) {
                    getLogger().info("Operation status Succeeded in temp table - {}", statuses.get(0).status.toString());
                    //if clustered and if ingestion succeeded then update status to success and if non clustered set flag to tempTableIngestion succeeded
                    if (isNifiClusteredSetup(getNodeTypeProvider())) {
                        Map<String, String> existingMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "SUCCEEDED");
                        context.getStateManager().setState(updatedMap, Scope.CLUSTER);
                        //getLogger().error("StateMap after updating success  - {}", context.getStateManager().getState(Scope.CLUSTER).toMap());
                    } else {
                        isSingleNodeTempTableIngestionSucceeded = true;
                    }
                }

                if (statuses.get(0).status == OperationStatus.Failed || statuses.get(0).status == OperationStatus.PartiallySucceeded) {
                    getLogger().error("Operation status Error - {}", statuses.get(0).status.toString());
                    if (isNifiClusteredSetup(getNodeTypeProvider())) {
                        Map<String, String> existingMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                        Map<String, String> updatedMap = new ConcurrentHashMap<>(existingMap);
                        updatedMap.put(getNodeTypeProvider().getCurrentNode().toString(), "FAILED");
                        context.getStateManager().setState(updatedMap, Scope.CLUSTER);
                        //getLogger().error("StateMap after updating failure  - {}", context.getStateManager().getState(Scope.CLUSTER).toMap());
                    }
                }

                //if clustered check if the all the nodes ingestion status succeeded
                //no of nodes in the cluster and success should be same
                //if yes proceed for ingestion to actual table
                //if pending, wait for sometime, with configurable timeout
                //if all failed/partially succeeded then rel-failure

                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    //getLogger().error("cluster member size  - {}", getNodeTypeProvider().getClusterMembers().size());
                    //getLogger().error("statemap size  - {}", context.getStateManager().getState(Scope.CLUSTER).toMap());
                    CompletableFuture<Integer> countFuture = new CompletableFuture<>();
                    ScheduledExecutorService countScheduler = Executors.newScheduledThreadPool(1);

                    Runnable countTask = () -> {
                        try {
                            Map<String, String> nodeMap = context.getStateManager().getState(Scope.CLUSTER).toMap();
                            //getLogger().error("Getting the status of nodeMap  - {}", context.getStateManager().getState(Scope.CLUSTER).toMap());
                            int pendingCount = nodeMap.size();
                            int succeededCount = 0;
                            for (Map.Entry<String, String> entry : nodeMap.entrySet()) {
                                if (entry.getValue().equals("SUCCEEDED")) {
                                    succeededCount++;
                                    pendingCount--;
                                    getLogger().error("Statemap inside loop values succeeded - {} and {}", succeededCount, pendingCount);
                                } else if (entry.getValue().equals("FAILED")) {
                                    pendingCount--;
                                    getLogger().error("Statemap inside loop values failed - {}", pendingCount);
                                }
                            }
                            if (pendingCount == 0) {
                                getLogger().error("Statemap inside completed task execution - {} and {}", succeededCount, pendingCount);
                                countFuture.complete(succeededCount);
                            }
                        } catch (Exception e) {
                            countFuture.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
                        }
                    };

                    countScheduler.scheduleWithFixedDelay(countTask, 1L, 2L, TimeUnit.SECONDS);
                    int succeededCount = countFuture.get(1800, TimeUnit.SECONDS);

                    shutDownScheduler(countScheduler);

                    //getLogger().error("Statemap final execution - {} and {}", succeededCount);

                    if (succeededCount == context.getStateManager().getState(Scope.CLUSTER).toMap().size()) {
                        //clustered temp table ingestion succeeds
                        getLogger().error("Clustered Ingestion : succededCount same as state size " + succeededCount);
                        isClusteredTempTableIngestionSucceeded = true;
                    } else {
                        //clustered temp table ingestion fails
                        getLogger().error("Clustered Ingestion : Exception occurred while ingesting data into the ADX temp tables, hence aborting ingestion to main table.");
                        isError = true;
                    }
                }

                if(isClusteredTempTableIngestionSucceeded || isSingleNodeTempTableIngestionSucceeded){
                    try{
                        StringBuilder moveExtentsQuery  = new StringBuilder().append(".move async extents all from table "+
                                ingestionPropertiesCreateTempTable.getTableName()  +" to table "+ ingestionProperties.getTableName());
                        if(shouldUseMaterializedViewFlag(ingestionPropertiesCreateTempTable.getDatabaseName(),ingestionPropertiesCreateTempTable.getTableName())){
                            moveExtentsQuery.append(" with(SetNewIngestionTime=true)");
                        }
                        String operationId = executeMoveExtentsAsyncOperation(ingestionPropertiesCreateTempTable.getDatabaseName(),moveExtentsQuery.toString());
                        String showOperationsQuery = ".show operations "+ operationId;
                        //KustoResultSetTable operationDetailsRes = executionClient.execute(ingestionPropertiesCreateTempTable.getDatabaseName(),showOperationsQuery).getPrimaryResults();
                        String stateCol = "State";
                        //String statusCol = "Status";
                        String completionStatus = pollAndFindExtentMergeAsyncOperation(ingestionPropertiesCreateTempTable.getDatabaseName(),showOperationsQuery,stateCol);
                        if(completionStatus.equalsIgnoreCase("Failed")){
                            getLogger().error("Error occurred while moving extents from temp tables to actual table");
                            isError = true;
                        }
                    }catch (DataServiceException | DataClientException | ExecutionException | TimeoutException | InterruptedException e) {
                        getLogger().error("Exception occurred while moving extents from temp tables to actual table");
                        isError = true;
                    }
                }else{
                    isError = true;
                }

            } catch (IngestionClientException | IngestionServiceException
                     | InterruptedException | ExecutionException | TimeoutException | IOException | DataServiceException
                     | DataClientException e) {
                getLogger().error("Transactional Mode : Exception occurred while ingesting data into ADX with exception {} ", e);
                isError =true;
            } finally {
                try {
                    dropTempTableIfExists(ingestionPropertiesCreateTempTable);
                } catch (DataServiceException | DataClientException e) {
                    getLogger().error("Exception occurred while dropping the temp tables used for transactional ingestion {} ", e);
                }
                if (isNifiClusteredSetup(getNodeTypeProvider())) {
                    try {
                        context.getStateManager().clear(Scope.CLUSTER);
                    } catch (IOException e) {
                        getLogger().error("Exception occurred while clearing the cluster state {} ", e);
                        isError = true;
                    }
                }
            }
        }else{
            // when the transactional flag is false or streaming ingestion
            try (final InputStream inputStream = session.read(flowFile)) {
                StreamSourceInfo actualTableStreamSourceInfo = new StreamSourceInfo(inputStream);
                StringBuilder ingestLogString = new StringBuilder().append("Ingesting with: ")
                        .append("dataFormat - ").append(ingestionProperties.getDataFormat()).append("|")
                        .append("ingestionMapping - ").append(ingestionProperties.getIngestionMapping().getIngestionMappingReference()).append("|")
                        .append("reportLevel - ").append(ingestionProperties.getReportLevel()).append("|")
                        .append("reportMethod - ").append(ingestionProperties.getReportMethod()).append("|")
                        .append("databaseName - ").append(ingestionProperties.getDatabaseName()).append("|")
                        .append("tableName - ").append(ingestionProperties.getTableName()).append("|")
                        .append("flushImmediately - ").append(ingestionProperties.getFlushImmediately());
                getLogger().info(ingestLogString.toString());

                IngestionResult result = ingestClient.ingestFromStream(actualTableStreamSourceInfo, ingestionProperties);
                List<IngestionStatus> statuses = result.getIngestionStatusCollection();
                if (StringUtils.equalsIgnoreCase(context.getProperty(WAIT_FOR_STATUS).getValue(), IngestionStatusEnum.ST_SUCCESS.name())) {
                    CompletableFuture<List<IngestionStatus>> future = new CompletableFuture<>();
                    ScheduledExecutorService statusScheduler = Executors.newScheduledThreadPool(1);
                    Runnable task = () -> {
                        try {
                            List<IngestionStatus> statuses1 = result.getIngestionStatusCollection();
                            if (statuses1.get(0).status == OperationStatus.Succeeded
                                    || statuses1.get(0).status == OperationStatus.Failed
                                    || statuses1.get(0).status == OperationStatus.PartiallySucceeded) {
                                future.complete(statuses1);
                            }
                        } catch (Exception e) {
                            future.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
                        }
                    };
                    statusScheduler.scheduleWithFixedDelay(task, 1, 2, TimeUnit.SECONDS);
                    statuses = future.get(1800, TimeUnit.SECONDS);
                    shutDownScheduler(statusScheduler);
                } else {
                    IngestionStatus status = new IngestionStatus();
                    status.status = OperationStatus.Succeeded;
                    statuses.set(0, status);
                }

                getLogger().info("Operation status: {} ", statuses.get(0).details);
                if (statuses.get(0).status == OperationStatus.Succeeded) {
                    getLogger().info("Operation status Succeeded - {}", statuses.get(0).status.toString());
                }

                if (statuses.get(0).status == OperationStatus.Failed) {
                    getLogger().error("Operation status Error - {}", statuses.get(0).status.toString());
                    isError = true;
                }

                if (statuses.get(0).status == OperationStatus.PartiallySucceeded) {
                    getLogger().error("Operation status Partially succeeded - {}", statuses.get(0).status.toString());
                    isError = true;
                }
            } catch (IngestionClientException | IngestionServiceException | StorageException | URISyntaxException
                     | InterruptedException | ExecutionException | TimeoutException | IOException e) {
                getLogger().error("Non Transactional/Streaming Ingestion mode : Exception occurred while ingesting data into ADX with exception {} ", e);
                isError =true;
            }
        }

        if (isError) {
            getLogger().error("Process failed - {}");
            session.transfer(flowFile, RL_FAILED);
        }else{
            getLogger().info("Process succeeded - {}");
            session.transfer(flowFile, RL_SUCCEEDED);
        }
    }

    private void shutDownScheduler(ScheduledExecutorService executorService) {
        executorService.shutdown();
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                // Cancel currently executing tasks forcefully
                executorService.shutdownNow();
                // Wait a while for tasks to respond to being cancelled
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    getLogger().error("Scheduler did not terminate");
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdownNow();
        }
    }

    protected boolean isStreamingPolicyEnabled(
            String entityType, String entityName, Client engineClient, String database) throws DataClientException, DataServiceException {
        KustoResultSetTable res = engineClient.execute(database, String.format(STREAMING_POLICY_SHOW_COMMAND, entityType, entityName)).getPrimaryResults();
        res.next();
        return res.getString("Policy") != null;
    }

    protected boolean isIngestorRole(String databaseName, String tableName, Client executionClient) {
        try {
            executionClient.execute(databaseName, String.format(FETCH_TABLE_COMMAND, tableName));
        } catch (DataServiceException | DataClientException err) {
            if ((err.getMessage() != null && err.getMessage().contains("Forbidden:")) || (err.getCause() != null && err.getCause().getMessage().contains("Forbidden:"))) {
                return false;
            }
        }
        return true;
    }

    protected String addModifyBatchingPolicy(IngestionProperties ingestionProperties, ProcessContext context) {
        //add/modify ingestion batching policy as per user inputs
        String showIngestionBatchingQuery = ".show table " + ingestionProperties.getTableName() + " policy ingestionbatching";
        KustoOperationResult kustoOperationResultIngestionBatching = null;
        String ingestionBatchingString = null;
        try {
            kustoOperationResultIngestionBatching = executionClient.execute(ingestionProperties.getDatabaseName(), showIngestionBatchingQuery);
            KustoResultSetTable mainTableResultIngestionBatching = kustoOperationResultIngestionBatching.getPrimaryResults();
            if (mainTableResultIngestionBatching.first()) {
                int columnIndex = mainTableResultIngestionBatching.findColumn("Policy");
                ingestionBatchingString = mainTableResultIngestionBatching.getString(columnIndex);
            }
            IngestionBatchingPolicy ingestionBatchingPolicy = new IngestionBatchingPolicy();
            if (StringUtils.isNotEmpty(ingestionBatchingString)) {
                ingestionBatchingPolicy = new ObjectMapper().readValue(ingestionBatchingString, IngestionBatchingPolicy.class);
            }
            ingestionBatchingPolicy.setMaximumNumberOfItems(Integer.parseInt(context.getProperty(MAX_BATCHING_NO_OF_ITEMS).getValue()));
            ingestionBatchingPolicy.setMaximumBatchingTimeSpan(context.getProperty(MAX_BATCHING_TIME_SPAN).getValue());
            ingestionBatchingPolicy.setMaximumRawDataSizeMB(Integer.parseInt(context.getProperty(MAX_BATCHING_RAW_DATA_SIZE_IN_MB).getValue()));

            //apply batching policy
            String batchingPolicyString = new ObjectMapper().writeValueAsString(ingestionBatchingPolicy);
            String applyBatchingPolicy = ".alter table Storms policy ingestionbatching ``` " + batchingPolicyString + " ```";
            executionClient.execute(ingestionProperties.getDatabaseName(), applyBatchingPolicy);
        } catch (DataServiceException | DataClientException | JsonProcessingException e) {
            getLogger().error("Error occurred while retrieving ingestion batching policy of main table");
            throw new ProcessException("Error occurred while retrieving ingestion batching policy of main table");
        }
        return ingestionBatchingString;
    }

    protected String showOriginalTableRetentionPolicy(IngestionProperties ingestionProperties) throws DataServiceException, DataClientException {
        String showTableSchema = ".show table " + ingestionProperties.getTableName() + " cslschema";
        KustoOperationResult kustoOperationResult = executionClient.execute(ingestionProperties.getDatabaseName(), showTableSchema);

        KustoResultSetTable mainTableResult = kustoOperationResult.getPrimaryResults();
        String columnsAsSchema = null;
        if (mainTableResult.first()) {
            int columnIndex = mainTableResult.findColumn("Schema");
            columnsAsSchema = mainTableResult.getString(columnIndex);
        }
        return columnsAsSchema;
    }

    protected void dropTempTableIfExists(IngestionProperties ingestionPropertiesCreateTempTable) throws DataServiceException, DataClientException {
        String dropTempTableIfExistsQuery = ".drop table " + ingestionPropertiesCreateTempTable.getTableName() + " ifexists";
        executionClient.execute(ingestionPropertiesCreateTempTable.getDatabaseName(), dropTempTableIfExistsQuery);
    }

    protected void createTempTable(IngestionProperties ingestionPropertiesCreateTempTable, IngestionProperties ingestionProperties, String columnsAsSchema)
            throws DataServiceException, DataClientException {
        String createTempTableQuery = ".create table " + ingestionPropertiesCreateTempTable.getTableName()
                + " based-on "+ ingestionProperties.getTableName()  +" with (docstring='sample-table', folder='TempTables') ";
        executionClient.execute(ingestionPropertiesCreateTempTable.getDatabaseName(), createTempTableQuery);
    }

    protected void alterTempTableRetentionPolicy(IngestionProperties ingestionPropertiesCreateTempTable,ProcessContext context) throws DataServiceException, DataClientException {
        String alterRetentionPolicyTempTableQuery =
                ".alter-merge table " + ingestionPropertiesCreateTempTable.getTableName() + " policy retention softdelete ="
                        + context.getProperty(TEMP_TABLE_SOFT_DELETE_RETENTION).getValue() + " recoverability = disabled";
        executionClient.execute(ingestionPropertiesCreateTempTable.getDatabaseName(), alterRetentionPolicyTempTableQuery);
    }

    protected void alterTempTableAutoDeletePolicy(IngestionProperties ingestionPropertiesCreateTempTable, String expiryDate) throws DataServiceException, DataClientException {
        String setAutoDeleteForTempTableQuery =
                ".alter table " + ingestionPropertiesCreateTempTable.getTableName() + " policy auto_delete @'{ \"ExpiryDate\" : \"" + expiryDate + "\", \"DeleteIfNotEmpty\": true }'";
        executionClient.execute(ingestionPropertiesCreateTempTable.getDatabaseName(), setAutoDeleteForTempTableQuery);
    }

    protected void applyTempTableBatchingPolicy(IngestionProperties ingestionProperties, String ingestionBatchingString) throws DataServiceException, DataClientException {
        if (StringUtils.isNotEmpty(ingestionBatchingString)) {
            String applyBatchingPolicyTempTable = ".alter table Storms policy ingestionbatching ``` " + ingestionBatchingString + " ```";
            executionClient.execute(ingestionProperties.getDatabaseName(), applyBatchingPolicyTempTable);
        }
    }

    protected boolean isNifiClusteredSetup(NodeTypeProvider nodeTypeProvider){
        return nodeTypeProvider.isClustered() && nodeTypeProvider.isConnected();
    }

    protected int countNoOfExtents(String databaseName,String tableName) throws DataServiceException, DataClientException {
        String countNoOfExtentsQuery = ".show table "+ tableName +" extents | count";
        KustoResultSetTable res =executionClient.execute(databaseName, countNoOfExtentsQuery).getPrimaryResults();
        res.next();
        return res.getInt(0);
    }

    protected int showNodesCount() throws DataServiceException, DataClientException {
        String countNoOfExtentsQuery = ".show cluster | count";
        KustoResultSetTable res =executionClient.execute(countNoOfExtentsQuery).getPrimaryResults();
        res.next();
        return res.getInt(0);
    }

    protected boolean shouldUseMaterializedViewFlag(String databaseName, String tableName) throws DataServiceException, DataClientException {
        String materializedViewQuery = ".show materialized-views | where SourceTable == '"+ tableName +"' | count";
        KustoResultSetTable res =executionClient.execute(databaseName,materializedViewQuery).getPrimaryResults();
        res.next();
        boolean isDestinationTableMaterializedViewSource  = res.getLong(0) > 0;
        if(isDestinationTableMaterializedViewSource){
            String tableEngineV3Query = ".show table "+ tableName+ " details | project todynamic(ShardingPolicy).UseShardEngine";
            KustoResultSetTable resV3 = executionClient.execute(databaseName,tableEngineV3Query).getPrimaryResults();
            resV3.next();
            return resV3.getBoolean(0);
        }else{
            return false;
        }
    }

    protected String executeMoveExtentsAsyncOperation(String databaseName, String moveExtentsQuery) throws DataServiceException, DataClientException {
        KustoResultSetTable res = executionClient.execute(databaseName,moveExtentsQuery.toString()).getPrimaryResults();
        res.next();
        return res.getString(0);
    }

    protected String pollAndFindExtentMergeAsyncOperation(final String databaseName, final String showOperationsQuery, final String stateCol)
            throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<String> moveExtentfuture = new CompletableFuture<>();
        ScheduledExecutorService statusScheduler = Executors.newScheduledThreadPool(1);
        Runnable moveExtentTask = () -> {
            try {
                KustoResultSetTable operationDetailsResTemp = executionClient.execute(databaseName,showOperationsQuery).getPrimaryResults();;
                operationDetailsResTemp.next();
                String operationStatus = operationDetailsResTemp.getString(stateCol);
                getLogger().info("Status of operation {} ",operationStatus);
                switch (operationStatus) {
                    case "Completed":
                        moveExtentfuture.complete("Completed");
                        break;
                    case "Failed":
                    case "PartiallySucceeded":
                    case "Canceled":
                    case "Throttled":
                    case "BadInput":
                    case "Abandoned":
                        moveExtentfuture.complete("Failed");
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                moveExtentfuture.completeExceptionally(new ProcessException("Error occurred while checking ingestion status", e));
            }
        };
        statusScheduler.scheduleWithFixedDelay(moveExtentTask, 1, 2, TimeUnit.SECONDS);
        String completionStatus = moveExtentfuture.get(1800, TimeUnit.SECONDS);
        shutDownScheduler(statusScheduler);
        return completionStatus;
    }

}
