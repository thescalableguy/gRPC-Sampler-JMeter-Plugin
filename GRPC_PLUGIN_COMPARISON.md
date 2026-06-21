# JMeter gRPC Sampler Plugin Comparison Report

This document compares the implemented **gRPC Request Sampler Plugin** with the most widely used community plugin, **`zalopay-oss/jmeter-grpc-plugin`** (often referred to as the standard JMeter gRPC plugin).

---

## Comparison Matrix

| Feature | `zalopay-oss/jmeter-grpc-plugin` | Our Implemented gRPC Sampler | Key Advantage of Our Implementation |
| :--- | :--- | :--- | :--- |
| **No Proto Upload Required (Reflection)** | ❌ **No support.** Requires copying `.proto` files to the JMeter environment or importing compiled stub Jars. |  **Fully Supported.** Discovers services and methods dynamically at runtime via target Server Reflection API. | Instantly test services without checking out or copying `.proto` files. |
| **Native Streaming Support** | ❌ **Limited.** Standard community samplers are exclusively unary or require custom Java client scripting. |  **Fully Native.** Supports Unary, Server Streaming, Client Streaming, and Bidirectional Streaming RPCs natively. | Execute complex streaming APIs directly from the JMeter thread loop. |
| **Async Channels (ThreadPool Executor)** | ❌ **None.** Spawns independent, expensive thread pools per channel, leading to CPU throttling. |  **Optimized Executor Pool.** Uses a shared daemon thread pool `grpc-async-channel-pool` across channels. | Prevents OS thread leaks and socket exhaustions during high concurrency. |
| **OpenTelemetry (OTel) Tracing** | ❌ **None.** Requires chaining complex third-party tools or interceptors. |  **Traceparent Injection.** Dynamically generates and injects standard W3C compliant `traceparent` headers. | Out-of-the-box trace propagation to downstream microservices. |
| **Dynamic Evaluations** | ⚠️ **Limited.** Variables are often parsed once at startup; dynamic functions can fail to evaluate per-iteration. |  **CompoundVariable Parsing.** Evaluates all headers, metadata, and payloads dynamically on every single call. | Supports `${__time()}`, `${__UUID()}`, and standard variable expressions on every request. |
| **Apple Silicon (osx-aarch_64) Support** | ❌ **Prone to failures.** Hardcoded to older `protoc` versions; fails on M1/M2/M3 Macs. |  **Out-of-the-box support.** Dynamically requests `protoc` version `3.25.1`, which pulls down native AArch64 binaries. | Zero compilation or runtime errors on modern Apple chips. |
| **Connection Pooling & Channel Reuse** | ⚠️ **Basic.** Channels are frequently recreated, leading to high connection latency under heavy load. |  **Production-Grade Pool.** Caches `ManagedChannel` matching host, port, TLS, keepalive, and timeout configurations. | High throughput and minimal socket footprint by reusing channels across threads. |
| **Keep-Alive Configuration** | ❌ **None.** Relies on default operating system socket policies. |  **Explicit Keep-Alives.** Exposes a configurable Keep-Alive field in milliseconds which sets Netty keepalive policies at the transport level. | Prevents channels from being silently dropped by middleboxes/load-balancers. |
| **Response Headers & Trailers Capture** | ❌ **Limited.** Does not extract and format gRPC-specific metadata response headers/trailers into JMeter's header fields. |  **Comprehensive.** Uses a client-side interceptor (`HeaderCaptureInterceptor`) to capture `onHeaders` and `onClose` trailer metadata. | Easily assert against response headers and trailers. |
| **Binary Metadata Headers (`-bin`)** | ❌ **No native support.** Requires manual pre-encoding to ASCII bytes. |  **Automatic.** Auto-detects keys ending in `-bin` and handles Base64 encoding/decoding natively. | Simplifies authentication and binary context passing (e.g., token passing). |
| **JSON Template Generation** | ⚠️ **Basic.** Can fail or trigger infinite loops on recursive definitions or complex Map structures. |  **Advanced.** Implements a cycle detector and map entry translator to cleanly generate placeholders. | One-click skeleton generation without editor freezing. |
| **Variable Extraction** | ❌ **None.** Requires users to chain separate, generic JMeter JSON Path extractors. |  **Integrated Extractor.** Bundles a dedicated `gRPC JSON Extractor` post-processor. | Supports multi-match (`-1`), random (`0`), and fallback defaults out-of-the-box. |

---

## Detailed Architectural Differences

### 1. Zero-Config Schema Discovery (Server Reflection)
*   **Community Plugin:** Users must download the service's `.proto` files, along with all dependencies/imports, specify import directories in the JMeter GUI, and compile them. This setup breaks easily if imports change.
*   **Our Plugin:** By enabling "Use Server Reflection", the plugin opens a dynamic stream to the server's reflection service, queries available services/methods, compiles the byte arrays into memory, and caches them. The client needs zero configuration files to perform load testing.

### 2. Multi-Mode Streaming
*   **Community Plugin:** To load test a streaming endpoint (e.g., streaming status updates), users must write custom Groovy/Java samplers, handling threading and stream synchronization manually.
*   **Our Plugin:** We parse list arrays and newline-separated inputs, and execute Unary, Server Streaming, Client Streaming, and Bidi Streaming using generic stubs. The sampler handles stream lifecycle operations synchronously inside the JMeter thread execution loop, complying with JMeter's core reporting model.

### 3. Dynamic Execution of Variables (CompoundVariable)
*   **Community Plugin:** Many plugins fetch string properties directly from properties maps. While JMeter evaluates variables initially, dynamic function invocations (like generating a timestamp or random string) are not re-evaluated for every request in a loop.
*   **Our Plugin:** We wrap every property getter inside a custom `evaluate()` call that executes a `CompoundVariable` lookup. This guarantees that variables and functions are parsed and executed fresh on every single thread iteration.
