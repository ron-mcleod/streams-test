package com.mcleodnet.test.rabbitmq.stream;

import java.util.concurrent.*;
import java.util.stream.LongStream;

import com.rabbitmq.stream.*;

import picocli.CommandLine;
import picocli.CommandLine.*;

@Command(name = "rabbitmq-streams", description = "test RabbitMQ streams performance")
public class Main implements Runnable {
    private static final long WAIT_MESSAGE_MILLIS = 100;
    private Environment environment = null;
    private Consumer consumer = null;
    private Producer producer = null;
    private LinkedTransferQueue<Message> queue;

    private record ConsumeResults(long count, long duration) {
    };

    @Parameters(index = "0", paramLabel = "command", description = "consume, produce, or delete")
    String command;

    @Option(names = {
            "-h" }, paramLabel = "host", defaultValue = "localhost")
    String host;

    @Option(names = { "-p" }, paramLabel = "port", defaultValue = "5552")
    int port;

    @Option(names = { "-u" }, paramLabel = "username", defaultValue = "test")
    String username;

    @Option(names = { "-pw" }, paramLabel = "password", defaultValue = "test")
    String password;

    @Option(names = { "-sn" }, paramLabel = "stream name", defaultValue = "test-stream")
    String streamName;

    @Option(names = { "-ml" }, paramLabel = "max length (bytes)", defaultValue = "3000000")
    long maxLengthBytes;

    @Option(names = { "-ms" }, paramLabel = "max segment (bytes)", defaultValue = "300000")
    long maxSegmentBytes;

    @Option(names = { "-pc" }, paramLabel = "produce count", defaultValue = "5000")
    int produceCount;

    @Option(names = { "-pd" }, paramLabel = "produce delay (millis)", defaultValue = "0")
    long produceDelay;

    @Option(names = { "-bs" }, paramLabel = "batch size", defaultValue = "1")
    int batchSize;

    @Option(names = { "-ps" }, paramLabel = "payload size", defaultValue = "50")
    int payloadSize;

    public void run() {
        try {
            createEnvironment();

            if ("consume".startsWith(command)) {
                createConsumerAndConsumeToQueue();
                var results = consumeFromQueue();
                reportConsume(results);
            } else if ("produce".startsWith(command)) {
                if (!streamExists()) {
                    createStream();
                }
                createProducer();
                produce();
            } else if ("delete".startsWith(command)) {
                deleteStream();
            }
        } catch (Exception e) {
            System.err.println("Error: exception=%s".formatted(
                    String.join(":", e.getClass().getSimpleName(), e.getMessage())));
        } finally {
            cleanup();
        }
    }

    void createEnvironment() {
        this.environment = Environment.builder()
                .host(this.host)
                .port(this.port)
                .addressResolver(address -> new Address(this.host, this.port))
                .username(this.username)
                .password(this.password)
                .virtualHost("/")
                .id("streams-test")
                .build();
    }

    boolean streamExists() {
        return this.environment.streamExists(streamName);
    }

    void createStream() throws StreamException {
        this.environment.streamCreator()
                .stream(this.streamName)
                .maxLengthBytes(ByteCapacity.B(this.maxLengthBytes))
                .maxSegmentSizeBytes(ByteCapacity.B(this.maxSegmentBytes))
                .create();
    }

    void createConsumerAndConsumeToQueue() {
        this.queue = new LinkedTransferQueue<Message>();
        this.consumer = this.environment.consumerBuilder()
                .stream(this.streamName)
                .offset(OffsetSpecification.first())
                .messageHandler((context, message) -> this.queue.add(message))
                .build();
    }

    ConsumeResults consumeFromQueue() throws InterruptedException, RuntimeException {
        if (!streamExists()) {
            throw new RuntimeException("stream '%s' does not exist".formatted(this.streamName));
        }
        long count = 0;
        long startTs = System.currentTimeMillis();
        Message message = null;
        while ((message = queue.poll(WAIT_MESSAGE_MILLIS, TimeUnit.MILLISECONDS)) != null) {
            count++;
        }
        long endTs = System.currentTimeMillis();
        return new ConsumeResults(count, (endTs - startTs) - WAIT_MESSAGE_MILLIS);
    }

    void deleteStream() throws StreamException {
        if (!this.environment.streamExists(streamName)) {
            return;
        }
        this.environment.deleteStream(streamName);
    }

    void reportConsume(ConsumeResults results) {
        var rate = results.duration == 0 ? 0 : (results.count() * 1000) / results.duration;
        System.out.println("count=%,d   duration=%,d ms   rate=%,d msg/sec".formatted(
                results.count(), results.duration(), rate));
    }

    void createProducer() {
        this.producer = this.environment.producerBuilder()
                .stream(this.streamName)
                .batchSize(batchSize)
                .build();
    }

    void produce() throws InterruptedException {
        final var payload = "X".repeat(this.payloadSize).getBytes();
        final var produceConfirmLatch = new CountDownLatch(this.produceCount);
        var pubIdSeed = System.currentTimeMillis();
        LongStream.range(0, this.produceCount).forEach(pubId -> {
            var message = this.producer.messageBuilder()
                    .addData(payload)
                    .build();
            this.producer.send(message, confirmationStatus -> produceConfirmLatch.countDown());
            delay(produceDelay);
        });
        produceConfirmLatch.await(10, TimeUnit.SECONDS);
    }

    void delay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // safe to ignore
        }
    }

    void cleanup() {
        if (this.consumer != null) {
            this.consumer.close();
        }
        if (this.producer != null) {
            this.producer.close();
        }
        if (this.environment != null) {
            this.environment.close();
        }
    }

    public static void main(String[] args) {
        new CommandLine(new Main()).execute(args);
    }
}
