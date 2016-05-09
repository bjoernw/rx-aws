/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macgyver.reactor.aws.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.macgyver.reactor.aws.AbstractReactorBridge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class KinesisReactorBridge extends AbstractReactorBridge {

    private static final Logger logger = LoggerFactory.getLogger(KinesisReactorBridge.class);
    private KinesisClientLibConfiguration kinesisConfig;

    private Worker worker;

    private AmazonKinesisAsyncClient asyncKinesisClient;

    private final AtomicInteger bridgeThreadNum = new AtomicInteger(0);

    private boolean parseJson = false;

    private CheckpointStrategy checkpointStrategy = new TimeIntervalCheckpointStrategy();

    private Supplier<String> streamArnSupplier = Suppliers.memoize(new StreamArnSupplier());

    private ObjectMapper mapper = new ObjectMapper();

    public class StreamArnSupplier implements Supplier<String> {

        @Override
        public String get() {
            return getKinesisClient().describeStream(getStreamName()).getStreamDescription().getStreamARN();
        }

    }

    public class KinesisRecord {

        private Record record;

        private JsonNode jsonBody = null;

        public Record getRecord() {
            return record;
        }

        public String getStreamName() {
            return kinesisConfig.getStreamName();
        }

        public String getStreamArn() {
            return KinesisReactorBridge.this.getStreamArn();
        }

        public KinesisReactorBridge getBridge() {
            return KinesisReactorBridge.this;
        }

        public String getSequenceNumber() {
            return getRecord().getSequenceNumber();
        }

        public InputStream getBodyAsInputStream() {
            return new ByteBufferBackedInputStream(getRecord().getData().duplicate());
        }

        public byte[] getBodyAsByteArray() {
            return toByteArray(getRecord().getData().duplicate());
        }

        public String getBodyAsString() {
            return new String(getBodyAsByteArray());
        }

        public synchronized JsonNode getBodyAsJson() {
            if (jsonBody == null) {
                try {
                    jsonBody = mapper.readTree(getBodyAsByteArray());
                } catch (IOException e) {
                    jsonBody = MissingNode.getInstance();
                    logger.warn("could not parse json body: " + e.toString());
                }
            }

            return jsonBody;
        }
    }

    private static byte[] toByteArray(ByteBuffer bb) {

        if (bb.hasArray()) {
            return bb.array();
        } else {
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            return bytes;
        }
    }

    class BridgeRecordProcessor implements IRecordProcessor {

        @Override
        public void initialize(InitializationInput initializationInput) {
            logger.info("initializing {} with {}", this, initializationInput);
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            logger.info("processRecords");
            processRecordsInput.getRecords().forEach(record -> {
                KinesisRecord kr = new KinesisRecord();
                kr.record = record;
                Event<KinesisRecord> event = Event.wrap(kr);
                getEventBus().notify(kr, event);
                boolean cp = checkpointStrategy.call(record);
                if (cp) {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("checkpointing app {} for stream {} at {}", kinesisConfig.getApplicationName(),
                                    kinesisConfig.getStreamName(), record.getSequenceNumber());
                        }
                        processRecordsInput.getCheckpointer().checkpoint(record);
                    } catch (RuntimeException | InvalidStateException | ShutdownException e) {
                        logger.error("problem with checkpoint", e);
                    }
                }
            });
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            logger.info("shutdown {}", shutdownInput);

        }

    }

    public static class Builder {

        private KinesisClientLibConfiguration kinesisConfig = null;
        private EventBus eventBus;
        private String arn;
        private boolean parseJson = false;
        private String appName;
        private String streamName;
        private String regionName;
        private AWSCredentialsProvider credentialsProvider;
        private Consumer<KinesisClientLibConfiguration> extraConfig;
        private String workerId;
        private CheckpointStrategy checkpointStrategy;

        public Builder withStreamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder withRegion(Regions region) {
            return withRegion(Region.getRegion(region));
        }

        public Builder withRegion(Region region) {
            return withRegion(region.getName());
        }

        public Builder withRegion(String region) {
            this.regionName = region;
            return this;
        }

        public Builder withAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder withEventBus(EventBus bus) {
            this.eventBus = bus;
            return this;
        }

        public Builder withJsonParsing(boolean b) {
            parseJson = b;
            return this;
        }

        public Builder withKinesisConfig(KinesisClientLibConfiguration cfg) {
            this.kinesisConfig = cfg;
            return this;
        }

        public Builder withCheckpointStrategy(CheckpointStrategy strategy) {
            this.checkpointStrategy = strategy;
            return this;
        }

        public Builder withAdditionalConfig(Consumer<KinesisClientLibConfiguration> cfg) {

            extraConfig = cfg;
            return this;
        }

        public KinesisReactorBridge build() {

            Preconditions.checkArgument(eventBus != null, "EventBus cannot be null");
            KinesisReactorBridge bridge = new KinesisReactorBridge();

            if (kinesisConfig == null) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "streamName must be set");
                if (credentialsProvider == null) {
                    credentialsProvider = new DefaultAWSCredentialsProviderChain();
                }
                if (workerId == null) {
                    try {
                        workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
                    } catch (UnknownHostException e) {
                        workerId = "127.0.0.1:" + bridge.getId();
                    }
                }
                Preconditions.checkArgument(appName != null, "appName must be set");
                kinesisConfig = new KinesisClientLibConfiguration(appName, streamName, credentialsProvider,
                        workerId);
                if (regionName != null) {
                    kinesisConfig.withRegionName(regionName);
                }
            }

            bridge.kinesisConfig = kinesisConfig;

            if (checkpointStrategy != null) {
                bridge.checkpointStrategy = checkpointStrategy;
            }

            if (extraConfig != null) {
                extraConfig.accept(bridge.kinesisConfig);
            }
            bridge.eventBus = eventBus;

            AmazonKinesisAsyncClient asyncClient = new AmazonKinesisAsyncClient(
                    kinesisConfig.getKinesisCredentialsProvider());

            if (kinesisConfig.getRegionName() != null) {
                asyncClient.setRegion(Region.getRegion(Regions.fromName(kinesisConfig.getRegionName())));
            }
            bridge.asyncKinesisClient = asyncClient;
            if (parseJson) {
                JsonParsingConsumer.apply(bridge);
            }

            logger.info("bridgeId  : {}", bridge.getId());
            logger.info("appName   : {}", kinesisConfig.getApplicationName());
            logger.info("streamName: {}", kinesisConfig.getStreamName());
            logger.info("regionName: {}", kinesisConfig.getRegionName());
            logger.info("workerId  : {}", kinesisConfig.getWorkerIdentifier());
            logger.info("streamArn : {}", bridge.getStreamArn());

            logger.info("created {} ... don't forget to call start()", bridge);
            return bridge;
        }
    }

    public KinesisReactorBridge start() {
        logger.info("starting {}...", this);
        IRecordProcessorFactory factory = () -> {
            BridgeRecordProcessor p = new BridgeRecordProcessor();
            logger.info("creating {}", p);
            return p;
        };

        Preconditions.checkNotNull(kinesisConfig);
        worker = new Worker.Builder()
                .recordProcessorFactory(factory)
                .config(kinesisConfig)
                .build();

        Thread t = new Thread(worker);
        t.setDaemon(true);
        t.setName("kinesis-bridge-" + bridgeThreadNum.getAndIncrement());
        t.start();

        return this;
    }

    protected KinesisReactorBridge() {

    }

    public String getArn() {
        return getStreamArn();
    }

    public String getStreamName() {
        if (kinesisConfig == null) {
            return null;
        }
        return kinesisConfig.getStreamName();
    }

    public KinesisClientLibConfiguration getKinesisClientLibConfiguration() {
        return kinesisConfig;
    }

    public String getStreamArn() {
        return streamArnSupplier.get();

    }

    public Selector eventsFromBridgeSelector() {
        return Selectors.predicate(p -> {
            if (p instanceof KinesisRecord) {
                return ((KinesisRecord) p).getBridge() == this;
            }
            return false;
        });
    }

    public AmazonKinesisAsyncClient getKinesisClient() {
        return asyncKinesisClient;
    }

    public void stop() {
        worker.shutdown();
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("streamName", getStreamName()).toString();
    }
}
