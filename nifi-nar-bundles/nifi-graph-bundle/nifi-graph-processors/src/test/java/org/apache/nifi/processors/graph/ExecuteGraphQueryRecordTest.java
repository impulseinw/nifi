package org.apache.nifi.processors.graph;

import groovy.json.JsonOutput;
import org.apache.nifi.processors.graph.util.InMemoryGraphClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class ExecuteGraphQueryRecordTest {
    private TestRunner runner;
    private JsonTreeReader reader;
    private InMemoryGraphClient graphClient;
    Map<String, String> enqueProperties = new HashMap<>();

    @Before
    public void setup() throws InitializationException {
        MockRecordWriter writer = new MockRecordWriter();
        reader = new JsonTreeReader();
        runner = TestRunners.newTestRunner(ExecuteGraphQueryRecord.class);
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.setProperty(ExecuteGraphQueryRecord.READER_SERVICE, "reader");
        runner.setProperty(ExecuteGraphQueryRecord.WRITER_SERVICE, "writer");

        runner.enableControllerService(writer);
        runner.enableControllerService(reader);

        graphClient = new InMemoryGraphClient();


        runner.addControllerService("graphClient", graphClient);

        runner.setProperty(ExecuteGraphQueryRecord.CLIENT_SERVICE, "graphClient");
        runner.enableControllerService(graphClient);
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, "[ 'testProperty': 'testResponse' ]");
        runner.assertValid();
        enqueProperties.put("graph.name", "graph");

    }

    @Test
    public void testFlowFileContent() throws IOException {
        List<Map> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("M", 1);
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript;
        submissionScript = "[ 'M': M[0] ]";

        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("M", "/M");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.REL_GRAPH).get(0);
        relGraph.assertContentEquals(ExecuteGraphQueryRecordTest.class.getResourceAsStream("/testFlowFileContent.json"));
    }

    @Test
    public void testFlowFileList() throws IOException {
        List<Map> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("M", new ArrayList<>(){
            {
                add(1);
                add(2);
                add(3);
            }
        });
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "[ " +
                "'M': M[0] " +
                "]";

        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("M", "/M");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.REL_GRAPH).get(0);
        relGraph.assertContentEquals(ExecuteGraphQueryRecordTest.class.getResourceAsStream("/testFlowFileList.json"));
    }

    @Test
    public void testComplexFlowFile() throws IOException {
        List<Map> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("tMap", "123");
        tempMap.put("L", new ArrayList<>(){
            {
                add(1);
                add(2);
                add(3);
            }
        });
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "Map<String, Object> vertexHashes = new HashMap()\n" +
                "vertexHashes.put('1234', tMap[0])\n" +
                "[ 'L': L[0], 'result': vertexHashes ]";
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        runner.setProperty("tMap", "/tMap");
        runner.setProperty("L", "/L");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.REL_GRAPH).get(0);
        relGraph.assertContentEquals(ExecuteGraphQueryRecordTest.class.getResourceAsStream("/testComplexFlowFile.json"));
    }

    @Test
    public void testAttributes() throws IOException {
        List<Map<String, Object>> test = new ArrayList<>();
        Map<String, Object> tempMap = new HashMap<>();
        tempMap.put("tMap", "123");
        test.add(tempMap);

        byte[] json = JsonOutput.toJson(test).getBytes();
        String submissionScript = "[ " +
                "'testProperty': testProperty " +
                "] ";
        runner.setProperty(ExecuteGraphQueryRecord.SUBMISSION_SCRIPT, submissionScript);
        Map<String, String> enqueProperties = new HashMap<>();
        enqueProperties.put("testProperty", "test");
        runner.enqueue(json, enqueProperties);

        runner.run();
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_GRAPH, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(ExecuteGraphQueryRecord.REL_FAILURE, 0);
        MockFlowFile relGraph = runner.getFlowFilesForRelationship(ExecuteGraphQueryRecord.REL_GRAPH).get(0);
        relGraph.assertContentEquals(ExecuteGraphQueryRecordTest.class.getResourceAsStream("/testAttributes.json"));
    }

}
