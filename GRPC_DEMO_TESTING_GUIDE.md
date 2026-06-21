# Demo Guide: Testing gRPC Requests in Apache JMeter

This guide provides step-by-step instructions on how to run a local demo to test the **gRPC Request Sampler** and **gRPC JSON Extractor** using the embedded test server and Apache JMeter.

---

## 1. Start the Demo gRPC Server

We have added a `main` entrypoint to [TestServer.java](file:///Users/unifiedops/Documents/grpc-jmeter/src/test/java/org/apache/jmeter/protocol/grpc/sampler/TestServer.java) that spins up an active gRPC mock server on port `50051`. 

Run the following command from the project root directory:

```bash
mvn exec:java -Dexec.mainClass="org.apache.jmeter.protocol.grpc.sampler.TestServer" -Dexec.classpathScope="test"
```

Once started, the terminal will print:
```text
=========================================================
gRPC Demo Test Server started on port 50051
Reflection is enabled.
Exposed Service: hello.HelloService
Methods available:
  1. SayHello (UNARY)
  2. SayHelloStream (SERVER STREAMING)
  3. SayHelloClientStream (CLIENT STREAMING)
  4. SayHelloBidiStream (BIDIRECTIONAL STREAMING)
=========================================================
Press ENTER to stop the server...
```

---

## 2. Load the Plugin into Apache JMeter

1. Package the fat shaded jar by running:
   ```bash
   mvn clean package -DskipTests
   ```
2. Copy the resulting jar to the JMeter lib folder:
   ```bash
   cp target/grpc-jmeter-sampler-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/apache-jmeter/lib/ext/
   ```
3. Restart or start Apache JMeter.

---

## 3. Create a Test Plan in JMeter

### Setup Thread Group
1. Right-click on **Test Plan** -> **Add** -> **Threads (Users)** -> **Thread Group**.
2. Keep threads count as `1` and loop count as `1`.

### Add the gRPC Request Sampler
1. Right-click on the **Thread Group** -> **Add** -> **Sampler** -> **gRPC Request**.
2. Configure the **Server Configuration** panel:
   * **Target Host**: `localhost`
   * **Port**: `50051`
   * **Use TLS**: *Unchecked*
   * **Connection Timeout (ms)**: `5000`
   * **Deadline Timeout (ms)**: `10000`
   * **Keep Alive (ms)**: *Leave empty*
   * **Enable OpenTelemetry Trace Context Injection (traceparent)**: *Checked*

3. Configure the **Proto Management** panel:
   * **Use Server Reflection**: *Checked* (this queries the running `TestServer` dynamically and populates the dropdowns without uploading a `.proto` file).
   * **Service**: Select `hello.HelloService`
   * **Method**: Select `SayHello`

### Add a View Results Tree Listener
1. Right-click on the **Thread Group** -> **Add** -> **Listener** -> **View Results Tree**.

---

## 4. Run Different RPC Type Demos

Select different methods from the dropdown and configure the JSON payload in the request editor:

### A. Unary RPC (Single Request, Single Response)
* **Method**: `SayHello`
* **Request Payload (JSON)**:
  ```json
  {
    "name": "JMeter Demo"
  }
  ```
* **Expected Response**:
  ```json
  {
    "greeting": "Hello, JMeter Demo!",
    "code": 200
  }
  ```

### B. Server Streaming RPC (Single Request, Multiple Responses)
* **Method**: `SayHelloStream`
* **Request Payload (JSON)**:
  ```json
  {
    "name": "StreamTester"
  }
  ```
* **Expected Response** (shows multiple responses read sequentially):
  ```json
  [
    {
      "greeting": "Hello, StreamTester (Stream #1)!",
      "code": 200
    },
    {
      "greeting": "Hello, StreamTester (Stream #2)!",
      "code": 200
    },
    {
      "greeting": "Hello, StreamTester (Stream #3)!",
      "code": 200
    }
  ]
  ```

### C. Client Streaming RPC (Multiple Requests as JSON Array, Single Response)
* **Method**: `SayHelloClientStream`
* **Request Payload (JSON)**:
  ```json
  [
    { "name": "UserA" },
    { "name": "UserB" },
    { "name": "UserC" }
  ]
  ```
* **Expected Response** (aggregated by the server):
  ```json
  {
    "greeting": "Hello, UserA, UserB, UserC!",
    "code": 200
  }
  ```

### D. Bidirectional Streaming RPC (Multiple Requests, Multiple Responses)
* **Method**: `SayHelloBidiStream`
* **Request Payload (JSON)**:
  ```json
  [
    { "name": "Ping" },
    { "name": "Pong" }
  ]
  ```
* **Expected Response**:
  ```json
  [
    {
      "greeting": "Bidi Hello, Ping!",
      "code": 200
    },
    {
      "greeting": "Bidi Hello, Pong!",
      "code": 200
    }
  ]
  ```

---

## 5. Test Advanced Features

### Dynamic Variables & Functions
You can use standard JMeter variable placeholders and functions anywhere in the request headers or payload.
1. Add a **custom-header** in the **gRPC Metadata Headers** table:
   * **Name**: `custom-header`
   * **Value**: `${__UUID()}` (generates a unique UUID per request).
2. Change the JSON payload to include a dynamic timestamp:
   ```json
   {
     "name": "User-${__time()}"
   }
   ```
3. Execute the sampler and view the Response Headers in the **View Results Tree** listener. You will see the custom UUID echoed back by the server as:
   `x-echo-custom: <generated-uuid>`

### Extracting Responses with gRPC JSON Extractor
To save response values to a JMeter variable for subsequent requests:
1. Right-click on the **gRPC Request** -> **Add** -> **Post Processors** -> **gRPC JSON Extractor**.
2. Configure:
   * **JSON Path Expression**: `$.greeting`
   * **Reference Name**: `myExtractedGreeting`
   * **Default Value**: `GreetingNotFound`
3. You can reference this extracted greeting in a subsequent sampler as `${myExtractedGreeting}`.
