package org.apache.jmeter.protocol.grpc.extractor;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GrpcJsonExtractorTest {
    private JMeterContext context;
    private JMeterVariables vars;

    @BeforeEach
    public void setUp() {
        context = JMeterContextService.getContext();
        vars = new JMeterVariables();
        context.setVariables(vars);
    }

    @Test
    public void testSingleExtraction() throws Exception {
        SampleResult res = new SampleResult();
        String json = "{\"id\": 456, \"status\": \"ACTIVE\", \"details\": {\"owner\": \"Charlie\"}}";
        res.setResponseData(json, "UTF-8");
        context.setPreviousResult(res);

        GrpcJsonExtractor extractor = new GrpcJsonExtractor();
        extractor.setVarNames("ownerVar, statusVar");
        extractor.setJsonPathExprs("$.details.owner, $.status");
        extractor.setMatchNumbers("1, 1");
        extractor.setDefaultValues("defaultOwner, defaultStatus");

        extractor.process();

        assertEquals("Charlie", vars.get("ownerVar"));
        assertEquals("ACTIVE", vars.get("statusVar"));
    }

    @Test
    public void testMultipleMatches() throws Exception {
        SampleResult res = new SampleResult();
        String json = "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";
        res.setResponseData(json, "UTF-8");
        context.setPreviousResult(res);

        GrpcJsonExtractor extractor = new GrpcJsonExtractor();
        extractor.setVarNames("usersList");
        extractor.setJsonPathExprs("$.users[*].name");
        extractor.setMatchNumbers("-1"); // Extract all
        extractor.setDefaultValues("noUser");

        extractor.process();

        assertEquals("Alice", vars.get("usersList_1"));
        assertEquals("Bob", vars.get("usersList_2"));
        assertEquals("2", vars.get("usersList_matchNr"));
    }

    @Test
    public void testDefaultValueFallback() throws Exception {
        SampleResult res = new SampleResult();
        String json = "{\"users\": []}";
        res.setResponseData(json, "UTF-8");
        context.setPreviousResult(res);

        GrpcJsonExtractor extractor = new GrpcJsonExtractor();
        extractor.setVarNames("missingVar");
        extractor.setJsonPathExprs("$.nonexistent");
        extractor.setMatchNumbers("1");
        extractor.setDefaultValues("fallback_val");

        extractor.process();

        assertEquals("fallback_val", vars.get("missingVar"));
    }
}
