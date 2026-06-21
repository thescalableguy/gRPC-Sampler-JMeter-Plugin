package org.apache.jmeter.protocol.grpc.sampler;

import com.github.os72.protocjar.Protoc;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles dynamic resolution and compilation of Proto files.
 * Uses protoc-jar to compile .proto files dynamically at runtime
 * and parses descriptors to obtain services and methods.
 */
public class ProtoResolver {
    private static final Logger log = LoggerFactory.getLogger(ProtoResolver.class);
    
    // Cache to prevent duplicate compilation of unchanged files
    private static final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    private static class CacheEntry {
        final long lastModified;
        final List<Descriptors.FileDescriptor> fileDescriptors;

        CacheEntry(long lastModified, List<Descriptors.FileDescriptor> fileDescriptors) {
            this.lastModified = lastModified;
            this.fileDescriptors = fileDescriptors;
        }
    }

    /**
     * Resolves services and methods from a main proto file and optional import paths.
     *
     * @param protoFilePath Path to the main .proto file
     * @param importPaths   Optional list of directories to look for imports
     * @return List of Service Descriptors found in the proto file
     * @throws IOException If compilation or file operations fail
     * @throws Descriptors.DescriptorValidationException If descriptor validation fails
     */
    public static List<Descriptors.ServiceDescriptor> resolveServices(
            String protoFilePath, List<String> importPaths)
            throws IOException, Descriptors.DescriptorValidationException {
        
        File protoFile = new File(protoFilePath);
        if (!protoFile.exists()) {
            throw new IOException("Proto file not found: " + protoFilePath);
        }

        String cacheKey = protoFile.getAbsolutePath() + (importPaths != null ? importPaths.toString() : "");
        long currentLastModified = protoFile.lastModified();

        CacheEntry cached = cache.get(cacheKey);
        if (cached != null && cached.lastModified == currentLastModified) {
            log.info("Using cached descriptors for: {}", protoFilePath);
            return extractServices(cached.fileDescriptors);
        }

        log.info("Compiling proto file dynamically: {}", protoFilePath);
        File tempDescFile = Files.createTempFile("grpc_jmeter_", ".desc").toFile();
        tempDescFile.deleteOnExit();

        try {
            List<String> args = new ArrayList<>();
            args.add("-v3.25.1");
            args.add("--descriptor_set_out=" + tempDescFile.getAbsolutePath());
            args.add("--include_imports");
            
            // Add primary proto file directory
            args.add("--proto_path=" + protoFile.getParentFile().getAbsolutePath());
            
            // Add user import paths
            if (importPaths != null) {
                for (String path : importPaths) {
                    if (path != null && !path.trim().isEmpty()) {
                        File dir = new File(path.trim());
                        if (dir.exists() && dir.isDirectory()) {
                            args.add("--proto_path=" + dir.getAbsolutePath());
                        }
                    }
                }
            }
            args.add(protoFile.getAbsolutePath());
            int exitCode;
            try {
                exitCode = Protoc.runProtoc(args.toArray(new String[0]));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("protoc compilation was interrupted", e);
            }
            if (exitCode != 0) {
                throw new IOException("protoc compilation failed with exit code: " + exitCode);
            }

            byte[] descData;
            try (InputStream in = new FileInputStream(tempDescFile);
                 ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int n;
                while ((n = in.read(buffer)) != -1) {
                    out.write(buffer, 0, n);
                }
                descData = out.toByteArray();
            }

            DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(descData);
            List<Descriptors.FileDescriptor> fileDescriptors = buildFileDescriptors(descriptorSet);
            
            cache.put(cacheKey, new CacheEntry(currentLastModified, fileDescriptors));
            return extractServices(fileDescriptors);
        } finally {
            if (tempDescFile.exists()) {
                tempDescFile.delete();
            }
        }
    }

    private static List<Descriptors.FileDescriptor> buildFileDescriptors(
            DescriptorProtos.FileDescriptorSet descriptorSet) throws Descriptors.DescriptorValidationException, IOException {
        
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
            throws Descriptors.DescriptorValidationException, IOException {
        
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
            throw new IOException("Cannot resolve dependency: " + name);
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

    private static List<Descriptors.ServiceDescriptor> extractServices(
            List<Descriptors.FileDescriptor> fileDescriptors) {
        List<Descriptors.ServiceDescriptor> services = new ArrayList<>();
        for (Descriptors.FileDescriptor fd : fileDescriptors) {
            services.addAll(fd.getServices());
        }
        return services;
    }

    /**
     * Clears the dynamic descriptor cache.
     */
    public static void clearCache() {
        cache.clear();
    }
}
