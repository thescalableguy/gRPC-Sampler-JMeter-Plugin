package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class GrpcClientTest {
    private static TestServer server;
    private static int port;
    
    private static Descriptors.MethodDescriptor sayHelloMethod;
    private static Descriptors.MethodDescriptor sayHelloStreamMethod;
    private static Descriptors.MethodDescriptor sayHelloClientStreamMethod;
    private static Descriptors.MethodDescriptor sayHelloBidiStreamMethod;

    @BeforeAll
    public static void setUp() throws Exception {
        // Find free port
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        File protoFile = new File("src/test/resources/hello.proto");

        // Start server
        server = new TestServer(port, false);
        server.start(protoFile.getAbsolutePath());

        // Resolve proto descriptors using Reflection!
        List<Descriptors.ServiceDescriptor> services = ReflectionResolver.resolveServices(
                "localhost", port, false, 5000, 0, true);
        
        Descriptors.ServiceDescriptor helloService = services.stream()
                .filter(s -> "hello.HelloService".equals(s.getFullName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("hello.HelloService not found"));
                
        sayHelloMethod = helloService.findMethodByName("SayHello");
        sayHelloStreamMethod = helloService.findMethodByName("SayHelloStream");
        sayHelloClientStreamMethod = helloService.findMethodByName("SayHelloClientStream");
        sayHelloBidiStreamMethod = helloService.findMethodByName("SayHelloBidiStream");
    }

    @AfterAll
    public static void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testReflectionResolver() {
        assertNotNull(sayHelloMethod);
        assertNotNull(sayHelloStreamMethod);
        assertNotNull(sayHelloClientStreamMethod);
        assertNotNull(sayHelloBidiStreamMethod);
    }

    @Test
    public void testUnarySuccessWithOtel() {
        GrpcClient client = new GrpcClient();
        ManagedChannel channel = GrpcChannelManager.getChannel("localhost", port, false, 5000, 0);

        String payload = "{\"name\": \"Alice\", \"age\": 30}";
        GrpcClient.GrpcResponse response = client.execute(
                channel, sayHelloMethod, payload, null, 5000, true); // OTel enabled

        assertNotNull(response);
        assertTrue(response.isSuccess(), "Call should be successful");
        assertEquals("OK", response.getStatusCode());
        assertTrue(response.getResponseJson().contains("Hello, Alice!"), "Response should contain greeting");
        assertTrue(response.getResponseHeaders().contains("x-server-received: true"));
    }

    @Test
    public void testServerStreamingSuccess() {
        GrpcClient client = new GrpcClient();
        ManagedChannel channel = GrpcChannelManager.getChannel("localhost", port, false, 5000, 0);

        String payload = "{\"name\": \"Alice\"}";
        GrpcClient.GrpcResponse response = client.execute(
                channel, sayHelloStreamMethod, payload, null, 5000, false);

        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatusCode());
        
        String json = response.getResponseJson();
        assertTrue(json.contains("RESPONSE #1"), "Should contain first response tag");
        assertTrue(json.contains("RESPONSE #2"), "Should contain second response tag");
        assertTrue(json.contains("RESPONSE #3"), "Should contain third response tag");
        assertTrue(json.contains("Hello, Alice (Stream #1)!"));
        assertTrue(json.contains("Hello, Alice (Stream #3)!"));
    }

    @Test
    public void testClientStreamingSuccess() {
        GrpcClient client = new GrpcClient();
        ManagedChannel channel = GrpcChannelManager.getChannel("localhost", port, false, 5000, 0);

        // Multi-payload JSON Array
        String payload = "[{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]";
        GrpcClient.GrpcResponse response = client.execute(
                channel, sayHelloClientStreamMethod, payload, null, 5000, false);

        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatusCode());
        assertTrue(response.getResponseJson().contains("Hello, Alice, Bob!"));
    }

    @Test
    public void testBidiStreamingSuccess() {
        GrpcClient client = new GrpcClient();
        ManagedChannel channel = GrpcChannelManager.getChannel("localhost", port, false, 5000, 0);

        // Multi-payload JSON Array
        String payload = "[{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]";
        GrpcClient.GrpcResponse response = client.execute(
                channel, sayHelloBidiStreamMethod, payload, null, 5000, false);

        assertNotNull(response);
        assertTrue(response.isSuccess());
        assertEquals("OK", response.getStatusCode());
        
        String json = response.getResponseJson();
        assertTrue(json.contains("RESPONSE #1"));
        assertTrue(json.contains("RESPONSE #2"));
        assertTrue(json.contains("Bidi Hello, Alice!"));
        assertTrue(json.contains("Bidi Hello, Bob!"));
    }
}
