package org.apache.jmeter.protocol.grpc.sampler;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContext;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

public class GrpcSamplerTest {
    private static TestServer server;
    private static int port;
    private static File protoFile;

    private GrpcSampler sampler;

    @BeforeAll
    public static void setUpServer() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        protoFile = new File("src/test/resources/hello.proto");
        server = new TestServer(port, false);
        server.start(protoFile.getAbsolutePath());
    }

    @AfterAll
    public static void tearDownServer() {
        if (server != null) {
            server.stop();
        }
    }

    @BeforeEach
    public void setUpSampler() {
        JMeterContext context = JMeterContextService.getContext();
        context.setVariables(new JMeterVariables());

        sampler = new GrpcSampler();
        sampler.setName("gRPC Sampler Test");
        sampler.setHost("localhost");
        sampler.setPort(String.valueOf(port));
        sampler.setUseTls(false);
        sampler.setServiceName("hello.HelloService");
        sampler.setMethodName("SayHello");
        sampler.setPayload("{\"name\": \"World\"}");
        sampler.setConnectionTimeout("3000");
        sampler.setDeadlineTimeout("5000");
    }

    @Test
    public void testSamplerSuccessWithProtoFile() {
        sampler.setUseReflection(false);
        sampler.setProtoPath(protoFile.getAbsolutePath());
        sampler.threadStarted();

        SampleResult result = sampler.sample(null);

        assertNotNull(result);
        assertTrue(result.isSuccessful(), "Sampler should succeed using proto file");
        assertEquals("OK", result.getResponseCode());
        assertTrue(result.getResponseDataAsString().contains("Hello, World!"));
    }

    @Test
    public void testSamplerSuccessWithReflection() {
        sampler.setUseReflection(true); // Reflection enabled
        sampler.setProtoPath(""); // No proto path uploaded
        sampler.threadStarted();

        SampleResult result = sampler.sample(null);

        assertNotNull(result);
        assertTrue(result.isSuccessful(), "Sampler should succeed using Server Reflection");
        assertEquals("OK", result.getResponseCode());
        assertTrue(result.getResponseDataAsString().contains("Hello, World!"));
    }

    @Test
    public void testSamplerDynamicVariables() {
        sampler.setUseReflection(true);
        sampler.setProtoPath("");
        sampler.setPayload("{\"name\": \"${nameVar}\"}"); // Dynamic payload evaluation
        
        JMeterContextService.getContext().getVariables().put("nameVar", "DynamicUser");
        sampler.threadStarted();

        SampleResult result = sampler.sample(null);

        assertNotNull(result);
        assertTrue(result.isSuccessful());
        assertTrue(result.getResponseDataAsString().contains("Hello, DynamicUser!"), "Dynamic variables should be evaluated");
    }
}
