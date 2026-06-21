package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProtoResolverTest {

    @Test
    public void testResolveServices() throws Exception {
        File protoFile = new File("src/test/resources/hello.proto");
        assertTrue(protoFile.exists(), "Test proto file must exist at src/test/resources/hello.proto");

        List<Descriptors.ServiceDescriptor> services = ProtoResolver.resolveServices(protoFile.getAbsolutePath(), null);
        assertNotNull(services);
        assertFalse(services.isEmpty(), "Resolved services list should not be empty");

        Descriptors.ServiceDescriptor helloService = null;
        for (Descriptors.ServiceDescriptor service : services) {
            if ("hello.HelloService".equals(service.getFullName())) {
                helloService = service;
                break;
            }
        }

        assertNotNull(helloService, "hello.HelloService service should be resolved");
        
        Descriptors.MethodDescriptor sayHelloMethod = helloService.findMethodByName("SayHello");
        assertNotNull(sayHelloMethod, "SayHello method should be found in HelloService");

        // Verify request type fields
        Descriptors.Descriptor requestType = sayHelloMethod.getInputType();
        assertEquals("HelloRequest", requestType.getName());
        assertNotNull(requestType.findFieldByName("name"));
        assertNotNull(requestType.findFieldByName("age"));
        assertNotNull(requestType.findFieldByName("hobbies"));
        assertNotNull(requestType.findFieldByName("tags"));

        // Verify response type fields
        Descriptors.Descriptor responseType = sayHelloMethod.getOutputType();
        assertEquals("HelloResponse", responseType.getName());
        assertNotNull(responseType.findFieldByName("greeting"));
        assertNotNull(responseType.findFieldByName("code"));
    }
}
