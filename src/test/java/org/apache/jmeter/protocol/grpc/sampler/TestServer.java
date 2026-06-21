package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.*;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.reflection.v1alpha.*;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An embedded dynamic gRPC server supporting reflection and streaming RPC modes for integration testing.
 */
public class TestServer {
    private static final Logger log = LoggerFactory.getLogger(TestServer.class);

    public static final Context.Key<Metadata> HEADERS_KEY = Context.key("request-headers");

    private final int port;
    private final boolean useTls;
    private Server server;
    private SelfSignedCertificate ssc;

    public TestServer(int port, boolean useTls) {
        this.port = port;
        this.useTls = useTls;
    }

    public void start(String protoPath) throws IOException, Descriptors.DescriptorValidationException, CertificateException {
        // Resolve descriptors dynamically
        List<Descriptors.ServiceDescriptor> services = ProtoResolver.resolveServices(protoPath, null);
        Descriptors.ServiceDescriptor helloService = null;
        for (Descriptors.ServiceDescriptor s : services) {
            if (s.getFullName().equals("hello.HelloService")) {
                helloService = s;
                break;
            }
        }

        if (helloService == null) {
            throw new IllegalArgumentException("hello.HelloService not found in proto path");
        }

        Descriptors.MethodDescriptor sayHelloMethod = helloService.findMethodByName("SayHello");
        Descriptors.Descriptor requestDesc = sayHelloMethod.getInputType();
        Descriptors.Descriptor responseDesc = sayHelloMethod.getOutputType();

        // Build dynamic ServerServiceDefinition with Unary and Streaming RPC handlers
        ServerServiceDefinition serviceDef = ServerServiceDefinition.builder("hello.HelloService")
                .addMethod(
                        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                                .setType(MethodDescriptor.MethodType.UNARY)
                                .setFullMethodName("hello.HelloService/SayHello")
                                .setRequestMarshaller(new GrpcClient.DynamicMessageMarshaller(requestDesc))
                                .setResponseMarshaller(new GrpcClient.DynamicMessageMarshaller(responseDesc))
                                .build(),
                        ServerCalls.asyncUnaryCall(new ServerCalls.UnaryMethod<DynamicMessage, DynamicMessage>() {
                            @Override
                            public void invoke(DynamicMessage request, StreamObserver<DynamicMessage> responseObserver) {
                                handleSayHello(request, responseObserver, responseDesc);
                            }
                        })
                )
                .addMethod(
                        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                                .setFullMethodName("hello.HelloService/SayHelloStream")
                                .setRequestMarshaller(new GrpcClient.DynamicMessageMarshaller(requestDesc))
                                .setResponseMarshaller(new GrpcClient.DynamicMessageMarshaller(responseDesc))
                                .build(),
                        ServerCalls.asyncServerStreamingCall(new ServerCalls.ServerStreamingMethod<DynamicMessage, DynamicMessage>() {
                            @Override
                            public void invoke(DynamicMessage request, StreamObserver<DynamicMessage> responseObserver) {
                                handleSayHelloStream(request, responseObserver, responseDesc);
                            }
                        })
                )
                .addMethod(
                        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                                .setType(MethodDescriptor.MethodType.CLIENT_STREAMING)
                                .setFullMethodName("hello.HelloService/SayHelloClientStream")
                                .setRequestMarshaller(new GrpcClient.DynamicMessageMarshaller(requestDesc))
                                .setResponseMarshaller(new GrpcClient.DynamicMessageMarshaller(responseDesc))
                                .build(),
                        ServerCalls.asyncClientStreamingCall(new ServerCalls.ClientStreamingMethod<DynamicMessage, DynamicMessage>() {
                            @Override
                            public StreamObserver<DynamicMessage> invoke(StreamObserver<DynamicMessage> responseObserver) {
                                return handleSayHelloClientStream(responseObserver, responseDesc);
                            }
                        })
                )
                .addMethod(
                        MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                                .setFullMethodName("hello.HelloService/SayHelloBidiStream")
                                .setRequestMarshaller(new GrpcClient.DynamicMessageMarshaller(requestDesc))
                                .setResponseMarshaller(new GrpcClient.DynamicMessageMarshaller(responseDesc))
                                .build(),
                        ServerCalls.asyncBidiStreamingCall(new ServerCalls.BidiStreamingMethod<DynamicMessage, DynamicMessage>() {
                            @Override
                            public StreamObserver<DynamicMessage> invoke(StreamObserver<DynamicMessage> responseObserver) {
                                return handleSayHelloBidiStream(responseObserver, responseDesc);
                            }
                        })
                )
                .build();

        // Header echo and validation interceptor
        ServerInterceptor headerInterceptor = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

                ServerCall<ReqT, RespT> wrappedCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void sendHeaders(Metadata responseHeaders) {
                        responseHeaders.put(Metadata.Key.of("x-server-received", Metadata.ASCII_STRING_MARSHALLER), "true");
                        
                        String customVal = headers.get(Metadata.Key.of("custom-header", Metadata.ASCII_STRING_MARSHALLER));
                        if (customVal != null) {
                            responseHeaders.put(Metadata.Key.of("x-echo-custom", Metadata.ASCII_STRING_MARSHALLER), customVal);
                        }
                        super.sendHeaders(responseHeaders);
                    }
                };

                Context context = Context.current().withValue(HEADERS_KEY, headers);
                return Contexts.interceptCall(context, wrappedCall, headers, next);
            }
        };

        ServerServiceDefinition interceptedService = ServerInterceptors.intercept(serviceDef, headerInterceptor);

        // Build list of service names and file descriptor byte-strings for dynamic reflection
        List<String> serviceNames = new ArrayList<>();
        List<ByteString> fileDescriptorProtos = new ArrayList<>();
        Set<String> processedFiles = new HashSet<>();

        for (Descriptors.ServiceDescriptor s : services) {
            serviceNames.add(s.getFullName());
            Descriptors.FileDescriptor fd = s.getFile();
            if (processedFiles.add(fd.getName())) {
                fileDescriptorProtos.add(fd.toProto().toByteString());
                addDependenciesRecursively(fd, fileDescriptorProtos, processedFiles);
            }
        }

        DynamicReflectionService reflectionService = new DynamicReflectionService(serviceNames, fileDescriptorProtos);

        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .addService(interceptedService)
                .addService(reflectionService); // Register our custom reflection service

        if (useTls) {
            ssc = new SelfSignedCertificate();
            builder.useTransportSecurity(ssc.certificate(), ssc.privateKey());
        }

        server = builder.build().start();
        log.info("Test server started on port {} (Reflection: ON, TLS: {})", port, useTls);
    }

    private void addDependenciesRecursively(Descriptors.FileDescriptor fd, List<ByteString> protos, Set<String> processed) {
        for (Descriptors.FileDescriptor dep : fd.getDependencies()) {
            if (processed.add(dep.getName())) {
                protos.add(dep.toProto().toByteString());
                addDependenciesRecursively(dep, protos, processed);
            }
        }
    }

    private void handleSayHello(DynamicMessage request, StreamObserver<DynamicMessage> responseObserver, Descriptors.Descriptor responseDesc) {
        try {
            String name = (String) request.getField(request.getDescriptorForType().findFieldByName("name"));

            if ("throw_invalid".equals(name)) {
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Invalid name parameter").asRuntimeException());
                return;
            } else if ("throw_unavailable".equals(name)) {
                responseObserver.onError(Status.UNAVAILABLE.withDescription("Service is currently offline").asRuntimeException());
                return;
            } else if ("throw_deadline".equals(name)) {
                try {
                    Thread.sleep(1200);
                } catch (InterruptedException ignored) {}
                responseObserver.onError(Status.DEADLINE_EXCEEDED.withDescription("Deadline exceeded").asRuntimeException());
                return;
            }

            int code = 200;
            String greeting = "Hello, " + name + "!";

            DynamicMessage response = DynamicMessage.newBuilder(responseDesc)
                    .setField(responseDesc.findFieldByName("greeting"), greeting)
                    .setField(responseDesc.findFieldByName("code"), code)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private void handleSayHelloStream(DynamicMessage request, StreamObserver<DynamicMessage> responseObserver, Descriptors.Descriptor responseDesc) {
        try {
            String name = (String) request.getField(request.getDescriptorForType().findFieldByName("name"));
            for (int i = 1; i <= 3; i++) {
                DynamicMessage response = DynamicMessage.newBuilder(responseDesc)
                        .setField(responseDesc.findFieldByName("greeting"), "Hello, " + name + " (Stream #" + i + ")!")
                        .setField(responseDesc.findFieldByName("code"), 200)
                        .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    private StreamObserver<DynamicMessage> handleSayHelloClientStream(
            StreamObserver<DynamicMessage> responseObserver, Descriptors.Descriptor responseDesc) {
        return new StreamObserver<DynamicMessage>() {
            private final List<String> names = new ArrayList<>();
            @Override
            public void onNext(DynamicMessage value) {
                String name = (String) value.getField(value.getDescriptorForType().findFieldByName("name"));
                names.add(name);
            }
            @Override
            public void onError(Throwable t) {
                log.error("Client streaming error", t);
            }
            @Override
            public void onCompleted() {
                String merged = String.join(", ", names);
                DynamicMessage response = DynamicMessage.newBuilder(responseDesc)
                        .setField(responseDesc.findFieldByName("greeting"), "Hello, " + merged + "!")
                        .setField(responseDesc.findFieldByName("code"), 200)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };
    }

    private StreamObserver<DynamicMessage> handleSayHelloBidiStream(
            StreamObserver<DynamicMessage> responseObserver, Descriptors.Descriptor responseDesc) {
        return new StreamObserver<DynamicMessage>() {
            @Override
            public void onNext(DynamicMessage value) {
                String name = (String) value.getField(value.getDescriptorForType().findFieldByName("name"));
                DynamicMessage response = DynamicMessage.newBuilder(responseDesc)
                        .setField(responseDesc.findFieldByName("greeting"), "Bidi Hello, " + name + "!")
                        .setField(responseDesc.findFieldByName("code"), 200)
                        .build();
                responseObserver.onNext(response);
            }
            @Override
            public void onError(Throwable t) {
                log.error("Bidi streaming error", t);
            }
            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
            try {
                server.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
        }
        if (ssc != null) {
            ssc.delete();
        }
    }

    public File getSscCertificate() {
        return ssc != null ? ssc.certificate() : null;
    }

    public static void main(String[] args) {
        try {
            int port = 50051;
            boolean useTls = false;
            if (args.length > 0) {
                port = Integer.parseInt(args[0]);
            }
            if (args.length > 1) {
                useTls = Boolean.parseBoolean(args[1]);
            }
            
            String protoPath = "src/test/resources/hello.proto";
            File protoFile = new File(protoPath);
            if (!protoFile.exists()) {
                protoPath = "grpc-jmeter/src/test/resources/hello.proto";
                protoFile = new File(protoPath);
            }
            if (!protoFile.exists()) {
                System.err.println("Could not find hello.proto. Please run from the project root directory.");
                System.exit(1);
            }

            TestServer server = new TestServer(port, useTls);
            server.start(protoFile.getAbsolutePath());
            System.out.println("=========================================================");
            System.out.println("gRPC Demo Test Server started on port " + port);
            System.out.println("Reflection is enabled.");
            System.out.println("Exposed Service: hello.HelloService");
            System.out.println("Methods available:");
            System.out.println("  1. SayHello (UNARY)");
            System.out.println("  2. SayHelloStream (SERVER STREAMING)");
            System.out.println("  3. SayHelloClientStream (CLIENT STREAMING)");
            System.out.println("  4. SayHelloBidiStream (BIDIRECTIONAL STREAMING)");
            System.out.println("=========================================================");
            System.out.println("Press ENTER to stop the server...");
            System.in.read();
            server.stop();
            System.out.println("Server stopped.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Custom programmatic reflection service to bypass the lack of compiled classpath classes.
     */
    private static class DynamicReflectionService implements BindableService {
        private final List<String> serviceNames;
        private final List<ByteString> fileDescriptorProtos;

        public DynamicReflectionService(List<String> serviceNames, List<ByteString> fileDescriptorProtos) {
            this.serviceNames = serviceNames;
            this.fileDescriptorProtos = fileDescriptorProtos;
        }

        @Override
        public ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder("grpc.reflection.v1alpha.ServerReflection")
                    .addMethod(
                            ServerReflectionGrpc.getServerReflectionInfoMethod(),
                            ServerCalls.asyncBidiStreamingCall(new ServerCalls.BidiStreamingMethod<ServerReflectionRequest, ServerReflectionResponse>() {
                                @Override
                                public StreamObserver<ServerReflectionRequest> invoke(StreamObserver<ServerReflectionResponse> responseObserver) {
                                    return new StreamObserver<ServerReflectionRequest>() {
                                        @Override
                                        public void onNext(ServerReflectionRequest request) {
                                            ServerReflectionResponse.Builder response = ServerReflectionResponse.newBuilder()
                                                    .setOriginalRequest(request);

                                            if (request.hasListServices()) {
                                                ListServiceResponse.Builder listRes = ListServiceResponse.newBuilder();
                                                for (String name : serviceNames) {
                                                    listRes.addService(ServiceResponse.newBuilder().setName(name).build());
                                                }
                                                // Include reflection service itself in service list to be complete
                                                listRes.addService(ServiceResponse.newBuilder().setName("grpc.reflection.v1alpha.ServerReflection").build());
                                                response.setListServicesResponse(listRes.build());
                                            } else if (request.hasFileContainingSymbol() || request.hasFileByFilename()) {
                                                FileDescriptorResponse.Builder fdRes = FileDescriptorResponse.newBuilder();
                                                for (ByteString bs : fileDescriptorProtos) {
                                                    fdRes.addFileDescriptorProto(bs);
                                                }
                                                response.setFileDescriptorResponse(fdRes.build());
                                            } else {
                                                response.setErrorResponse(ErrorResponse.newBuilder()
                                                        .setErrorCode(Status.Code.UNIMPLEMENTED.value())
                                                        .setErrorMessage("Unsupported reflection request")
                                                        .build());
                                            }
                                            responseObserver.onNext(response.build());
                                        }

                                        @Override
                                        public void onError(Throwable t) {}

                                        @Override
                                        public void onCompleted() {
                                            responseObserver.onCompleted();
                                        }
                                    };
                                }
                            })
                    )
                    .build();
        }
    }
}
