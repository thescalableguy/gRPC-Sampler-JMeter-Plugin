package org.apache.jmeter.protocol.grpc.sampler;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.grpc.ManagedChannel;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Dynamically queries available services and methods from a gRPC server's reflection endpoint.
 */
public class ReflectionResolver {
    private static final Logger log = LoggerFactory.getLogger(ReflectionResolver.class);
    
    // Cache reflection resolved descriptors by host:port:useTls
    private static final Map<String, List<Descriptors.ServiceDescriptor>> cache = new ConcurrentHashMap<>();

    /**
     * Resolves services and methods from a target server's reflection endpoint.
     */
    public static List<Descriptors.ServiceDescriptor> resolveServices(
            String host, int port, boolean useTls, long connectionTimeoutMs, long keepAliveMs, boolean forceRefresh)
            throws Exception {
        
        String cacheKey = host + ":" + port + ":" + useTls;
        if (!forceRefresh && cache.containsKey(cacheKey)) {
            log.info("Using cached reflection descriptors for: {}:{}", host, port);
            return cache.get(cacheKey);
        }

        log.info("Querying server reflection at: {}:{}", host, port);
        ManagedChannel channel = GrpcChannelManager.getChannel(host, port, useTls, connectionTimeoutMs, keepAliveMs);
        List<Descriptors.ServiceDescriptor> services = queryServerReflection(channel);
        cache.put(cacheKey, services);
        return services;
    }

    private static List<Descriptors.ServiceDescriptor> queryServerReflection(ManagedChannel channel) throws Exception {
        ServerReflectionGrpc.ServerReflectionStub stub = ServerReflectionGrpc.newStub(channel);
        
        // 1. Fetch available services
        List<String> serviceNames = listServices(stub);
        log.info("Discovered services via reflection: {}", serviceNames);

        // 2. Fetch FileDescriptorProtos for all services
        Set<ByteString> descriptorBytes = new LinkedHashSet<>();
        for (String serviceName : serviceNames) {
            if ("grpc.reflection.v1alpha.ServerReflection".equals(serviceName)) {
                continue; // Skip standard reflection API service
            }
            List<ByteString> fileDescProtos = getFileDescriptorsForSymbol(stub, serviceName);
            descriptorBytes.addAll(fileDescProtos);
        }

        // 3. Compile FileDescriptorSet
        DescriptorProtos.FileDescriptorSet.Builder setBuilder = DescriptorProtos.FileDescriptorSet.newBuilder();
        for (ByteString bytes : descriptorBytes) {
            setBuilder.addFile(DescriptorProtos.FileDescriptorProto.parseFrom(bytes));
        }
        DescriptorProtos.FileDescriptorSet fdSet = setBuilder.build();

        // 4. Resolve FileDescriptors recursively
        List<Descriptors.FileDescriptor> fileDescriptors = buildFileDescriptors(fdSet);
        
        List<Descriptors.ServiceDescriptor> services = new ArrayList<>();
        for (Descriptors.FileDescriptor fd : fileDescriptors) {
            services.addAll(fd.getServices());
        }
        return services;
    }

    private static List<String> listServices(ServerReflectionGrpc.ServerReflectionStub stub) throws Exception {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        StreamObserver<ServerReflectionRequest> requestObserver = stub.serverReflectionInfo(new StreamObserver<ServerReflectionResponse>() {
            private final List<String> services = new ArrayList<>();

            @Override
            public void onNext(ServerReflectionResponse value) {
                if (value.hasListServicesResponse()) {
                    for (int i = 0; i < value.getListServicesResponse().getServiceCount(); i++) {
                        services.add(value.getListServicesResponse().getService(i).getName());
                    }
                    future.complete(services);
                } else if (value.hasErrorResponse()) {
                    future.completeExceptionally(new IOException("Reflection error: " + value.getErrorResponse().getErrorMessage()));
                }
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!future.isDone()) {
                    future.complete(services);
                }
            }
        });

        requestObserver.onNext(ServerReflectionRequest.newBuilder().setListServices("*").build());
        requestObserver.onCompleted();

        return future.get(10, TimeUnit.SECONDS);
    }

    private static List<ByteString> getFileDescriptorsForSymbol(ServerReflectionGrpc.ServerReflectionStub stub, String symbol) throws Exception {
        CompletableFuture<List<ByteString>> future = new CompletableFuture<>();
        StreamObserver<ServerReflectionRequest> requestObserver = stub.serverReflectionInfo(new StreamObserver<ServerReflectionResponse>() {
            private final List<ByteString> descriptors = new ArrayList<>();

            @Override
            public void onNext(ServerReflectionResponse value) {
                if (value.hasFileDescriptorResponse()) {
                    for (ByteString bs : value.getFileDescriptorResponse().getFileDescriptorProtoList()) {
                        descriptors.add(bs);
                    }
                    future.complete(descriptors);
                } else if (value.hasErrorResponse()) {
                    future.completeExceptionally(new IOException("Reflection error: " + value.getErrorResponse().getErrorMessage()));
                }
            }

            @Override
            public void onError(Throwable t) {
                future.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                if (!future.isDone()) {
                    future.complete(descriptors);
                }
            }
        });

        requestObserver.onNext(ServerReflectionRequest.newBuilder().setFileContainingSymbol(symbol).build());
        requestObserver.onCompleted();

        return future.get(10, TimeUnit.SECONDS);
    }

    private static List<Descriptors.FileDescriptor> buildFileDescriptors(
            DescriptorProtos.FileDescriptorSet descriptorSet) throws Exception {
        
        Map<String, DescriptorProtos.FileDescriptorProto> protoMap = new HashMap<>();
        for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
            protoMap.put(fdp.getName(), fdp);
        }

        Map<String, Descriptors.FileDescriptor> resolvedMap = new HashMap<>();
        for (DescriptorProtos.FileDescriptorProto fdp : descriptorSet.getFileList()) {
            resolveFile(fdp.getName(), protoMap, resolvedMap);
        }

        return new ArrayList<>(resolvedMap.values());
    }

    private static Descriptors.FileDescriptor resolveFile(
            String name,
            Map<String, DescriptorProtos.FileDescriptorProto> protoMap,
            Map<String, Descriptors.FileDescriptor> resolvedMap)
            throws Exception {
        
        if (resolvedMap.containsKey(name)) {
            return resolvedMap.get(name);
        }

        DescriptorProtos.FileDescriptorProto fdp = protoMap.get(name);
        if (fdp == null) {
            Descriptors.FileDescriptor standard = getStandardDescriptor(name);
            if (standard != null) {
                resolvedMap.put(name, standard);
                return standard;
            }
            throw new IOException("Cannot resolve reflection dependency: " + name);
        }

        List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
        for (String depName : fdp.getDependencyList()) {
            Descriptors.FileDescriptor depFd = resolveFile(depName, protoMap, resolvedMap);
            if (depFd != null) {
                dependencies.add(depFd);
            }
        }

        Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(
                fdp, dependencies.toArray(new Descriptors.FileDescriptor[0]));
        resolvedMap.put(name, fd);
        return fd;
    }

    private static Descriptors.FileDescriptor getStandardDescriptor(String name) {
        switch (name) {
            case "google/protobuf/any.proto": return com.google.protobuf.AnyProto.getDescriptor();
            case "google/protobuf/api.proto": return com.google.protobuf.ApiProto.getDescriptor();
            case "google/protobuf/descriptor.proto": return com.google.protobuf.DescriptorProtos.getDescriptor();
            case "google/protobuf/duration.proto": return com.google.protobuf.DurationProto.getDescriptor();
            case "google/protobuf/empty.proto": return com.google.protobuf.EmptyProto.getDescriptor();
            case "google/protobuf/field_mask.proto": return com.google.protobuf.FieldMaskProto.getDescriptor();
            case "google/protobuf/source_context.proto": return com.google.protobuf.SourceContextProto.getDescriptor();
            case "google/protobuf/struct.proto": return com.google.protobuf.StructProto.getDescriptor();
            case "google/protobuf/timestamp.proto": return com.google.protobuf.TimestampProto.getDescriptor();
            case "google/protobuf/type.proto": return com.google.protobuf.TypeProto.getDescriptor();
            case "google/protobuf/wrappers.proto": return com.google.protobuf.WrappersProto.getDescriptor();
            default: return null;
        }
    }

    public static void clearCache() {
        cache.clear();
    }
}
