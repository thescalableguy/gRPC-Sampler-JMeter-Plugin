package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.util.JsonFormat;
import io.grpc.*;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import com.jayway.jsonpath.JsonPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Executes dynamic gRPC requests (Unary, Server Streaming, Client Streaming, and Bidi Streaming) using standard ManagedChannels.
 */
public class GrpcClient {
    private static final Logger log = LoggerFactory.getLogger(GrpcClient.class);

    /**
     * Executes a gRPC request depending on the RPC type.
     */
    public GrpcResponse execute(
            ManagedChannel channel,
            Descriptors.MethodDescriptor methodDescriptor,
            String jsonPayload,
            Map<String, String> metadataHeaders,
            long deadlineTimeoutMs,
            boolean enableOtel) {

        GrpcResponse grpcResponse = new GrpcResponse();
        long startTime = System.nanoTime();
        HeaderCaptureInterceptor headerCapture = new HeaderCaptureInterceptor();

        try {
            // Determine RPC Type from descriptor
            MethodDescriptor.MethodType rpcType = MethodDescriptor.MethodType.UNARY;
            if (methodDescriptor.isClientStreaming() && methodDescriptor.isServerStreaming()) {
                rpcType = MethodDescriptor.MethodType.BIDI_STREAMING;
            } else if (methodDescriptor.isClientStreaming()) {
                rpcType = MethodDescriptor.MethodType.CLIENT_STREAMING;
            } else if (methodDescriptor.isServerStreaming()) {
                rpcType = MethodDescriptor.MethodType.SERVER_STREAMING;
            }

            // Parse request payloads
            List<DynamicMessage> requestMessages = new ArrayList<>();
            String trimmedPayload = jsonPayload != null ? jsonPayload.trim() : "";
            
            if (rpcType == MethodDescriptor.MethodType.CLIENT_STREAMING || rpcType == MethodDescriptor.MethodType.BIDI_STREAMING) {
                if (trimmedPayload.startsWith("[")) {
                    try {
                        List<Map<String, Object>> items = JsonPath.read(trimmedPayload, "$");
                        for (Map<String, Object> item : items) {
                            String itemJson = mapToJson(item);
                            DynamicMessage.Builder requestBuilder = DynamicMessage.newBuilder(methodDescriptor.getInputType());
                            JsonFormat.parser().ignoringUnknownFields().merge(itemJson, requestBuilder);
                            requestMessages.add(requestBuilder.build());
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse JSON array payload for streaming, falling back to split by newline: {}", e.getMessage());
                    }
                }
                
                if (requestMessages.isEmpty()) {
                    String[] lines = trimmedPayload.split("\n");
                    for (String line : lines) {
                        if (!line.trim().isEmpty()) {
                            DynamicMessage.Builder requestBuilder = DynamicMessage.newBuilder(methodDescriptor.getInputType());
                            JsonFormat.parser().ignoringUnknownFields().merge(line, requestBuilder);
                            requestMessages.add(requestBuilder.build());
                        }
                    }
                }
            }

            // Fallback for Unary/Server Streaming or failed array parse
            if (requestMessages.isEmpty()) {
                DynamicMessage.Builder requestBuilder = DynamicMessage.newBuilder(methodDescriptor.getInputType());
                if (!trimmedPayload.isEmpty()) {
                    JsonFormat.parser().ignoringUnknownFields().merge(trimmedPayload, requestBuilder);
                }
                requestMessages.add(requestBuilder.build());
            }

            long totalRequestSize = 0;
            for (DynamicMessage msg : requestMessages) {
                totalRequestSize += msg.getSerializedSize();
            }

            // Prepare custom metadata headers
            Metadata metadata = new Metadata();
            if (metadataHeaders != null) {
                for (Map.Entry<String, String> entry : metadataHeaders.entrySet()) {
                    String keyStr = entry.getKey();
                    String valueStr = entry.getValue();
                    if (keyStr != null && !keyStr.trim().isEmpty() && valueStr != null) {
                        if (keyStr.endsWith("-bin")) {
                            Metadata.Key<byte[]> key = Metadata.Key.of(keyStr, Metadata.BINARY_BYTE_MARSHALLER);
                            metadata.put(key, Base64.getDecoder().decode(valueStr.trim()));
                        } else {
                            Metadata.Key<String> key = Metadata.Key.of(keyStr, Metadata.ASCII_STRING_MARSHALLER);
                            metadata.put(key, valueStr);
                        }
                    }
                }
            }

            // OTel Traceparent Injection
            if (enableOtel) {
                String traceId = UUID.randomUUID().toString().replace("-", "");
                String spanId = String.format("%016x", new Random().nextLong());
                String traceparent = "00-" + traceId + "-" + spanId + "-01";
                metadata.put(Metadata.Key.of("traceparent", Metadata.ASCII_STRING_MARSHALLER), traceparent);
            }

            // Estimate request metadata size
            long requestMetadataSize = getMetadataSize(metadata);
            grpcResponse.setBytesSent(totalRequestSize + requestMetadataSize);

            // Intercept channel
            Channel interceptedChannel = ClientInterceptors.intercept(
                    channel,
                    MetadataUtils.newAttachHeadersInterceptor(metadata),
                    headerCapture
            );

            // Create generic dynamic MethodDescriptor for gRPC
            MethodDescriptor<DynamicMessage, DynamicMessage> grpcMethod = MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                    .setType(rpcType)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(
                            methodDescriptor.getService().getFullName(), methodDescriptor.getName()))
                    .setRequestMarshaller(new DynamicMessageMarshaller(methodDescriptor.getInputType()))
                    .setResponseMarshaller(new DynamicMessageMarshaller(methodDescriptor.getOutputType()))
                    .build();

            // Configure call options
            CallOptions callOptions = CallOptions.DEFAULT;
            if (deadlineTimeoutMs > 0) {
                callOptions = callOptions.withDeadlineAfter(deadlineTimeoutMs, TimeUnit.MILLISECONDS);
            }

            // Execute based on RPC Type
            String responseJson = "";
            long responseSize = 0;

            if (rpcType == MethodDescriptor.MethodType.UNARY) {
                DynamicMessage responseMessage = ClientCalls.blockingUnaryCall(
                        interceptedChannel, grpcMethod, callOptions, requestMessages.get(0));
                responseSize = responseMessage != null ? responseMessage.getSerializedSize() : 0;
                if (responseMessage != null) {
                    responseJson = JsonFormat.printer().includingDefaultValueFields().print(responseMessage);
                }
            } else if (rpcType == MethodDescriptor.MethodType.SERVER_STREAMING) {
                Iterator<DynamicMessage> responseIterator = ClientCalls.blockingServerStreamingCall(
                        interceptedChannel, grpcMethod, callOptions, requestMessages.get(0));
                StringBuilder sb = new StringBuilder();
                int count = 0;
                while (responseIterator.hasNext()) {
                    DynamicMessage msg = responseIterator.next();
                    responseSize += msg.getSerializedSize();
                    sb.append("--- RESPONSE #").append(++count).append(" ---\n");
                    sb.append(JsonFormat.printer().includingDefaultValueFields().print(msg)).append("\n\n");
                }
                responseJson = sb.toString();
            } else if (rpcType == MethodDescriptor.MethodType.CLIENT_STREAMING) {
                CompletableFuture<DynamicMessage> responseFuture = new CompletableFuture<>();
                ClientCall<DynamicMessage, DynamicMessage> call = interceptedChannel.newCall(grpcMethod, callOptions);
                StreamObserver<DynamicMessage> requestObserver = ClientCalls.asyncClientStreamingCall(
                        call, new StreamObserver<DynamicMessage>() {
                            @Override
                            public void onNext(DynamicMessage value) {
                                responseFuture.complete(value);
                            }
                            @Override
                            public void onError(Throwable t) {
                                responseFuture.completeExceptionally(t);
                            }
                            @Override
                            public void onCompleted() {}
                        });

                for (DynamicMessage msg : requestMessages) {
                    requestObserver.onNext(msg);
                }
                requestObserver.onCompleted();

                long timeout = deadlineTimeoutMs > 0 ? deadlineTimeoutMs : 10000;
                DynamicMessage responseMessage = responseFuture.get(timeout, TimeUnit.MILLISECONDS);
                responseSize = responseMessage != null ? responseMessage.getSerializedSize() : 0;
                if (responseMessage != null) {
                    responseJson = JsonFormat.printer().includingDefaultValueFields().print(responseMessage);
                }
            } else if (rpcType == MethodDescriptor.MethodType.BIDI_STREAMING) {
                List<DynamicMessage> responses = Collections.synchronizedList(new ArrayList<>());
                CompletableFuture<Void> completedFuture = new CompletableFuture<>();
                
                ClientCall<DynamicMessage, DynamicMessage> call = interceptedChannel.newCall(grpcMethod, callOptions);
                StreamObserver<DynamicMessage> requestObserver = ClientCalls.asyncBidiStreamingCall(
                        call, new StreamObserver<DynamicMessage>() {
                            @Override
                            public void onNext(DynamicMessage value) {
                                responses.add(value);
                            }
                            @Override
                            public void onError(Throwable t) {
                                completedFuture.completeExceptionally(t);
                            }
                            @Override
                            public void onCompleted() {
                                completedFuture.complete(null);
                            }
                        });

                for (DynamicMessage msg : requestMessages) {
                    requestObserver.onNext(msg);
                }
                requestObserver.onCompleted();

                long timeout = deadlineTimeoutMs > 0 ? deadlineTimeoutMs : 10000;
                completedFuture.get(timeout, TimeUnit.MILLISECONDS);

                StringBuilder sb = new StringBuilder();
                int count = 0;
                for (DynamicMessage msg : responses) {
                    responseSize += msg.getSerializedSize();
                    sb.append("--- RESPONSE #").append(++count).append(" ---\n");
                    sb.append(JsonFormat.printer().includingDefaultValueFields().print(msg)).append("\n\n");
                }
                responseJson = sb.toString();
            }

            long elapsedNanos = System.nanoTime() - startTime;
            grpcResponse.setResponseJson(responseJson);
            grpcResponse.setSuccess(true);
            grpcResponse.setStatusCode(Status.Code.OK.name());
            grpcResponse.setStatusDescription("OK");
            grpcResponse.setElapsedNanos(elapsedNanos);

            Metadata resHeaders = headerCapture.getResponseHeaders();
            Metadata resTrailers = headerCapture.getResponseTrailers();
            grpcResponse.setResponseHeaders(formatMetadata(resHeaders));
            grpcResponse.setResponseTrailers(formatMetadata(resTrailers));

            long responseMetadataSize = getMetadataSize(resHeaders) + getMetadataSize(resTrailers);
            grpcResponse.setBytesReceived(responseSize + responseMetadataSize);

        } catch (StatusRuntimeException e) {
            long elapsedNanos = System.nanoTime() - startTime;
            grpcResponse.setElapsedNanos(elapsedNanos);
            grpcResponse.setSuccess(false);
            grpcResponse.setStatusCode(e.getStatus().getCode().name());
            grpcResponse.setStatusDescription(e.getStatus().getDescription() != null ? e.getStatus().getDescription() : e.getStatus().toString());

            Metadata resHeaders = headerCapture.getResponseHeaders();
            Metadata resTrailers = headerCapture.getResponseTrailers();
            grpcResponse.setResponseHeaders(formatMetadata(resHeaders));
            grpcResponse.setResponseTrailers(formatMetadata(resTrailers));

            long responseMetadataSize = getMetadataSize(resHeaders) + getMetadataSize(resTrailers);
            grpcResponse.setBytesReceived(responseMetadataSize);

            log.error("gRPC execution failed: Status={}, Description={}", e.getStatus().getCode(), e.getStatus().getDescription());
        } catch (Exception e) {
            long elapsedNanos = System.nanoTime() - startTime;
            grpcResponse.setElapsedNanos(elapsedNanos);
            grpcResponse.setSuccess(false);
            grpcResponse.setStatusCode(Status.Code.UNKNOWN.name());
            grpcResponse.setStatusDescription(e.getMessage() != null ? e.getMessage() : e.toString());
            log.error("Generic execution failed in gRPC call", e);
        }

        return grpcResponse;
    }

    private static String mapToJson(Object obj) {
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            StringBuilder sb = new StringBuilder("{");
            int count = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                sb.append("\"").append(entry.getKey()).append("\":").append(mapToJson(entry.getValue()));
                if (++count < map.size()) {
                    sb.append(",");
                }
            }
            sb.append("}");
            return sb.toString();
        } else if (obj instanceof List) {
            List<?> list = (List<?>) obj;
            StringBuilder sb = new StringBuilder("[");
            int count = 0;
            for (Object item : list) {
                sb.append(mapToJson(item));
                if (++count < list.size()) {
                    sb.append(",");
                }
            }
            sb.append("]");
            return sb.toString();
        } else if (obj instanceof String) {
            return "\"" + obj.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r") + "\"";
        } else if (obj == null) {
            return "null";
        } else {
            return obj.toString();
        }
    }

    private static long getMetadataSize(Metadata metadata) {
        if (metadata == null) return 0;
        long size = 0;
        for (String keyName : metadata.keys()) {
            size += keyName.getBytes().length;
            if (keyName.endsWith("-bin")) {
                Metadata.Key<byte[]> key = Metadata.Key.of(keyName, Metadata.BINARY_BYTE_MARSHALLER);
                byte[] val = metadata.get(key);
                if (val != null) {
                    size += val.length;
                }
            } else {
                Metadata.Key<String> key = Metadata.Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER);
                String val = metadata.get(key);
                if (val != null) {
                    size += val.getBytes().length;
                }
            }
        }
        return size;
    }

    private static String formatMetadata(Metadata metadata) {
        if (metadata == null) return "";
        StringBuilder sb = new StringBuilder();
        for (String keyName : metadata.keys()) {
            if (keyName.endsWith("-bin")) {
                Metadata.Key<byte[]> key = Metadata.Key.of(keyName, Metadata.BINARY_BYTE_MARSHALLER);
                byte[] val = metadata.get(key);
                if (val != null) {
                    sb.append(keyName).append(": ").append(Base64.getEncoder().encodeToString(val)).append("\n");
                }
            } else {
                Metadata.Key<String> key = Metadata.Key.of(keyName, Metadata.ASCII_STRING_MARSHALLER);
                String val = metadata.get(key);
                if (val != null) {
                    sb.append(keyName).append(": ").append(val).append("\n");
                }
            }
        }
        return sb.toString();
    }

    /**
     * Interceptor to capture response headers and trailers asynchronously during block.
     */
    private static class HeaderCaptureInterceptor implements ClientInterceptor {
        private final Metadata responseHeaders = new Metadata();
        private final Metadata responseTrailers = new Metadata();

        public Metadata getResponseHeaders() {
            return responseHeaders;
        }

        public Metadata getResponseTrailers() {
            return responseTrailers;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        @Override
                        public void onHeaders(Metadata headers) {
                            responseHeaders.merge(headers);
                            super.onHeaders(headers);
                        }

                        @Override
                        public void onClose(Status status, Metadata trailers) {
                            if (trailers != null) {
                                responseTrailers.merge(trailers);
                            }
                            super.onClose(status, trailers);
                        }
                    }, headers);
                }
            };
        }
    }

    /**
     * Marshaller to serialize and deserialize dynamic protobuf messages dynamically.
     */
    public static class DynamicMessageMarshaller implements MethodDescriptor.Marshaller<DynamicMessage> {
        private final Descriptors.Descriptor messageDescriptor;

        public DynamicMessageMarshaller(Descriptors.Descriptor messageDescriptor) {
            this.messageDescriptor = messageDescriptor;
        }

        @Override
        public InputStream stream(DynamicMessage value) {
            return value.toByteString().newInput();
        }

        @Override
        public DynamicMessage parse(InputStream stream) {
            try {
                return DynamicMessage.parseFrom(messageDescriptor, stream);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize dynamic message: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Structure representing the metrics and content of a gRPC execution response.
     */
    public static class GrpcResponse {
        private String responseJson = "";
        private String statusCode = "UNKNOWN";
        private String statusDescription = "";
        private boolean isSuccess = false;
        private String responseHeaders = "";
        private String responseTrailers = "";
        private long elapsedNanos = 0;
        private long bytesSent = 0;
        private long bytesReceived = 0;

        public String getResponseJson() { return responseJson; }
        public void setResponseJson(String responseJson) { this.responseJson = responseJson; }

        public String getStatusCode() { return statusCode; }
        public void setStatusCode(String statusCode) { this.statusCode = statusCode; }

        public String getStatusDescription() { return statusDescription; }
        public void setStatusDescription(String statusDescription) { this.statusDescription = statusDescription; }

        public boolean isSuccess() { return isSuccess; }
        public void setSuccess(boolean success) { isSuccess = success; }

        public String getResponseHeaders() { return responseHeaders; }
        public void setResponseHeaders(String responseHeaders) { this.responseHeaders = responseHeaders; }

        public String getResponseTrailers() { return responseTrailers; }
        public void setResponseTrailers(String responseTrailers) { this.responseTrailers = responseTrailers; }

        public long getElapsedNanos() { return elapsedNanos; }
        public void setElapsedNanos(long elapsedNanos) { this.elapsedNanos = elapsedNanos; }

        public long getBytesSent() { return bytesSent; }
        public void setBytesSent(long bytesSent) { this.bytesSent = bytesSent; }

        public long getBytesReceived() { return bytesReceived; }
        public void setBytesReceived(long bytesReceived) { this.bytesReceived = bytesReceived; }
    }
}
