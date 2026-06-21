package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;
import io.grpc.ManagedChannel;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.engine.util.CompoundVariable;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.ThreadListener;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * JMeter Sampler for executing gRPC requests.
 */
public class GrpcSampler extends AbstractSampler implements ThreadListener {
    private static final Logger log = LoggerFactory.getLogger(GrpcSampler.class);

    public static final String HOST = "GrpcSampler.host";
    public static final String PORT = "GrpcSampler.port";
    public static final String USE_TLS = "GrpcSampler.useTls";
    public static final String CONNECTION_TIMEOUT = "GrpcSampler.connectionTimeout";
    public static final String DEADLINE_TIMEOUT = "GrpcSampler.deadlineTimeout";
    public static final String KEEP_ALIVE = "GrpcSampler.keepAlive";
    
    public static final String USE_REFLECTION = "GrpcSampler.useReflection";
    public static final String ENABLE_OTEL = "GrpcSampler.enableOtel";
    
    public static final String PROTO_PATH = "GrpcSampler.protoPath";
    public static final String IMPORT_PATHS = "GrpcSampler.importPaths";
    public static final String SERVICE_NAME = "GrpcSampler.serviceName";
    public static final String METHOD_NAME = "GrpcSampler.methodName";
    
    public static final String METADATA = "GrpcSampler.metadata";
    public static final String PAYLOAD = "GrpcSampler.payload";

    private transient GrpcClient grpcClient;

    public GrpcSampler() {
        setMetadata(new Arguments());
    }

    private GrpcClient getGrpcClient() {
        if (grpcClient == null) {
            grpcClient = new GrpcClient();
        }
        return grpcClient;
    }

    private String evaluate(String value) {
        if (value == null) return "";
        return new CompoundVariable(value).execute();
    }

    @Override
    public SampleResult sample(Entry entry) {
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());
        
        // Dynamically evaluate all variables for every request invocation
        String host = evaluate(getHost());
        String portStr = evaluate(getPort());
        boolean useTls = isUseTls();
        String connTimeoutStr = evaluate(getConnectionTimeout());
        String deadlineTimeoutStr = evaluate(getDeadlineTimeout());
        String keepAliveStr = evaluate(getKeepAlive());
        
        boolean useReflection = isUseReflection();
        boolean enableOtel = isEnableOtel();
        
        String protoPath = evaluate(getProtoPath());
        String importPaths = evaluate(getImportPaths());
        String serviceName = evaluate(getServiceName());
        String methodName = evaluate(getMethodName());
        String payload = evaluate(getPayload());

        // 1. Validate mandatory fields
        if (host.isEmpty() || portStr.isEmpty() || (!useReflection && protoPath.isEmpty()) || serviceName.isEmpty() || methodName.isEmpty()) {
            res.setResponseCode("INVALID_CONFIG");
            res.setResponseMessage("Missing mandatory configuration fields.");
            res.setSuccessful(false);
            return res;
        }

        int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            res.setResponseCode("INVALID_PORT");
            res.setResponseMessage("Port must be a valid number: " + portStr);
            res.setSuccessful(false);
            return res;
        }

        long connTimeout = 0;
        if (!connTimeoutStr.isEmpty()) {
            try {
                connTimeout = Long.parseLong(connTimeoutStr);
            } catch (NumberFormatException ignored) {}
        }

        long deadlineTimeout = 0;
        if (!deadlineTimeoutStr.isEmpty()) {
            try {
                deadlineTimeout = Long.parseLong(deadlineTimeoutStr);
            } catch (NumberFormatException ignored) {}
        }

        long keepAlive = 0;
        if (!keepAliveStr.isEmpty()) {
            try {
                keepAlive = Long.parseLong(keepAliveStr);
            } catch (NumberFormatException ignored) {}
        }

        // Parse custom metadata/headers
        Map<String, String> headers = new HashMap<>();
        Arguments metadataArgs = getMetadata();
        if (metadataArgs != null) {
            for (int i = 0; i < metadataArgs.getArgumentCount(); i++) {
                String key = metadataArgs.getArgument(i).getName();
                String val = metadataArgs.getArgument(i).getValue();
                if (key != null && !key.trim().isEmpty()) {
                    headers.put(evaluate(key), evaluate(val));
                }
            }
        }

        // Set request data in JMeter
        res.setSamplerData(String.format("gRPC Request to: %s:%d/%s/%s\n\nHeaders:\n%s\n\nPayload:\n%s", 
                host, port, serviceName, methodName, headers, payload));
        res.setRequestHeaders(headers.toString());

        // 2. Resolve Service and Method Descriptors
        Descriptors.MethodDescriptor methodDescriptor = null;
        try {
            List<Descriptors.ServiceDescriptor> services;
            if (useReflection) {
                services = ReflectionResolver.resolveServices(host, port, useTls, connTimeout, keepAlive, false);
            } else {
                List<String> importPathsList = new ArrayList<>();
                if (!importPaths.isEmpty()) {
                    for (String path : importPaths.split(",")) {
                        importPathsList.add(path.trim());
                    }
                }
                services = ProtoResolver.resolveServices(protoPath, importPathsList);
            }

            for (Descriptors.ServiceDescriptor service : services) {
                if (service.getFullName().equals(serviceName)) {
                    methodDescriptor = service.findMethodByName(methodName);
                    break;
                }
            }
        } catch (Exception e) {
            res.setResponseCode("RESOLVE_ERROR");
            res.setResponseMessage("Failed to resolve service schema: " + e.getMessage());
            res.setSuccessful(false);
            return res;
        }

        if (methodDescriptor == null) {
            res.setResponseCode("METHOD_NOT_FOUND");
            res.setResponseMessage(String.format("Service/Method '%s/%s' not found in service registry.", serviceName, methodName));
            res.setSuccessful(false);
            return res;
        }

        // 3. Obtain channel and execute request
        ManagedChannel channel;
        try {
            channel = GrpcChannelManager.getChannel(host, port, useTls, connTimeout, keepAlive);
        } catch (Exception e) {
            res.setResponseCode("CONNECTION_ERROR");
            res.setResponseMessage("Failed to initialize gRPC channel: " + e.getMessage());
            res.setSuccessful(false);
            return res;
        }

        // Start call measuring
        res.sampleStart();
        GrpcClient.GrpcResponse grpcResponse = getGrpcClient().execute(
                channel,
                methodDescriptor,
                payload,
                headers,
                deadlineTimeout,
                enableOtel
        );
        res.sampleEnd();

        // 4. Populate JMeter SampleResult
        res.setResponseData(grpcResponse.getResponseJson(), "UTF-8");
        res.setResponseCode(grpcResponse.getStatusCode());
        res.setResponseMessage(grpcResponse.getStatusDescription());
        res.setSuccessful(grpcResponse.isSuccess());
        res.setResponseHeaders(grpcResponse.getResponseHeaders());
        
        long latencyMs = grpcResponse.getElapsedNanos() / 1_000_000;
        res.setLatency(latencyMs);
        res.setConnectTime(0);
        res.setSentBytes(grpcResponse.getBytesSent());
        res.setBytes(grpcResponse.getBytesReceived());
        
        return res;
    }

    // --- Property Getters / Setters ---

    public String getHost() {
        return getPropertyAsString(HOST);
    }

    public void setHost(String host) {
        setProperty(HOST, host);
    }

    public String getPort() {
        return getPropertyAsString(PORT);
    }

    public void setPort(String port) {
        setProperty(PORT, port);
    }

    public boolean isUseTls() {
        return getPropertyAsBoolean(USE_TLS);
    }

    public void setUseTls(boolean useTls) {
        setProperty(USE_TLS, useTls);
    }

    public String getConnectionTimeout() {
        return getPropertyAsString(CONNECTION_TIMEOUT);
    }

    public void setConnectionTimeout(String timeout) {
        setProperty(CONNECTION_TIMEOUT, timeout);
    }

    public String getDeadlineTimeout() {
        return getPropertyAsString(DEADLINE_TIMEOUT);
    }

    public void setDeadlineTimeout(String deadline) {
        setProperty(DEADLINE_TIMEOUT, deadline);
    }

    public String getKeepAlive() {
        return getPropertyAsString(KEEP_ALIVE);
    }

    public void setKeepAlive(String keepAlive) {
        setProperty(KEEP_ALIVE, keepAlive);
    }

    public boolean isUseReflection() {
        return getPropertyAsBoolean(USE_REFLECTION, false);
    }

    public void setUseReflection(boolean useReflection) {
        setProperty(USE_REFLECTION, useReflection);
    }

    public boolean isEnableOtel() {
        return getPropertyAsBoolean(ENABLE_OTEL, false);
    }

    public void setEnableOtel(boolean enableOtel) {
        setProperty(ENABLE_OTEL, enableOtel);
    }

    public String getProtoPath() {
        return getPropertyAsString(PROTO_PATH);
    }

    public void setProtoPath(String protoPath) {
        setProperty(PROTO_PATH, protoPath);
    }

    public String getImportPaths() {
        return getPropertyAsString(IMPORT_PATHS);
    }

    public void setImportPaths(String importPaths) {
        setProperty(IMPORT_PATHS, importPaths);
    }

    public String getServiceName() {
        return getPropertyAsString(SERVICE_NAME);
    }

    public void setServiceName(String serviceName) {
        setProperty(SERVICE_NAME, serviceName);
    }

    public String getMethodName() {
        return getPropertyAsString(METHOD_NAME);
    }

    public void setMethodName(String methodName) {
        setProperty(METHOD_NAME, methodName);
    }

    public Arguments getMetadata() {
        return (Arguments) getProperty(METADATA).getObjectValue();
    }

    public void setMetadata(Arguments arguments) {
        setProperty(new TestElementProperty(METADATA, arguments));
    }

    public String getPayload() {
        return getPropertyAsString(PAYLOAD);
    }

    public void setPayload(String payload) {
        setProperty(PAYLOAD, payload);
    }

    // --- ThreadListener interface implementation ---

    @Override
    public void threadStarted() {
        grpcClient = new GrpcClient();
    }

    @Override
    public void threadFinished() {}
}
