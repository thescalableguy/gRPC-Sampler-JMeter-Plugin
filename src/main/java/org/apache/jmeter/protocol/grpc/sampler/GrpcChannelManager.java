package org.apache.jmeter.protocol.grpc.sampler;

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages caching and reuse of ManagedChannels to optimize performance.
 */
public class GrpcChannelManager {
    private static final Logger log = LoggerFactory.getLogger(GrpcChannelManager.class);
    private static final Map<ChannelKey, ManagedChannel> channelPool = new ConcurrentHashMap<>();

    private static final ExecutorService sharedExecutor = Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors() * 2),
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger();
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "grpc-async-channel-pool-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            }
    );

    static {
        // Register shutdown hook to clean up channels on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(GrpcChannelManager::shutdownAll));
    }

    /**
     * Gets a ManagedChannel from the pool, or creates one if it doesn't exist.
     */
    public static ManagedChannel getChannel(String host, int port, boolean useTls, long connectionTimeoutMs, long keepAliveMs) {
        ChannelKey key = new ChannelKey(host, port, useTls, connectionTimeoutMs, keepAliveMs);
        return channelPool.computeIfAbsent(key, GrpcChannelManager::createNewChannel);
    }

    private static ManagedChannel createNewChannel(ChannelKey key) {
        log.info("Creating new ManagedChannel for gRPC: host={}, port={}, useTls={}, connectionTimeoutMs={}, keepAliveMs={}",
                key.host, key.port, key.useTls, key.connectionTimeoutMs, key.keepAliveMs);
        
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(key.host, key.port);
        builder.executor(sharedExecutor);
        
        if (key.useTls) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }

        if (key.connectionTimeoutMs > 0) {
            builder.withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) key.connectionTimeoutMs);
        }

        if (key.keepAliveMs > 0) {
            builder.keepAliveTime(key.keepAliveMs, TimeUnit.MILLISECONDS);
            builder.keepAliveTimeout(20, TimeUnit.SECONDS);
            builder.keepAliveWithoutCalls(true);
        }

        return builder.build();
    }

    /**
     * Shuts down all active channels in the pool.
     */
    public static synchronized void shutdownAll() {
        log.info("Shutting down all cached gRPC channels...");
        for (Map.Entry<ChannelKey, ManagedChannel> entry : channelPool.entrySet()) {
            ManagedChannel channel = entry.getValue();
            if (!channel.isShutdown()) {
                try {
                    channel.shutdown();
                    if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    log.warn("Channel termination interrupted for key: {}", entry.getKey());
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
        channelPool.clear();
        sharedExecutor.shutdown();
        try {
            if (!sharedExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                sharedExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            sharedExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("All gRPC channels shut down.");
    }

    private static class ChannelKey {
        private final String host;
        private final int port;
        private final boolean useTls;
        private final long connectionTimeoutMs;
        private final long keepAliveMs;

        public ChannelKey(String host, int port, boolean useTls, long connectionTimeoutMs, long keepAliveMs) {
            this.host = host;
            this.port = port;
            this.useTls = useTls;
            this.connectionTimeoutMs = connectionTimeoutMs;
            this.keepAliveMs = keepAliveMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChannelKey that = (ChannelKey) o;
            return port == that.port &&
                    useTls == that.useTls &&
                    connectionTimeoutMs == that.connectionTimeoutMs &&
                    keepAliveMs == that.keepAliveMs &&
                    Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port, useTls, connectionTimeoutMs, keepAliveMs);
        }

        @Override
        public String toString() {
            return "ChannelKey{" +
                    "host='" + host + '\'' +
                    ", port=" + port +
                    ", useTls=" + useTls +
                    ", connectionTimeoutMs=" + connectionTimeoutMs +
                    ", keepAliveMs=" + keepAliveMs +
                    '}';
        }
    }
}
