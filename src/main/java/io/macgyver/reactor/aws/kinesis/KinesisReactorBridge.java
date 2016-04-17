/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macgyver.reactor.aws.kinesis;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.macgyver.reactor.aws.AbstractReactorBridge;
import reactor.bus.Event;
import reactor.bus.Event.Headers;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class KinesisReactorBridge extends AbstractReactorBridge {

	static Logger logger = LoggerFactory.getLogger(KinesisReactorBridge.class);
	KinesisClientLibConfiguration kinesisConfig;

	DateTimeFormatter arrivalFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.of("UTC"));

	Worker worker;

	AmazonKinesisAsyncClient asyncKinesisClient;
	String arn;

	AtomicInteger bridgeThreadNum = new AtomicInteger(0);

	AtomicLong lastCheckpoint = new AtomicLong(0);

	long checkpointIntervalMillis = TimeUnit.SECONDS.toMillis(30);

	boolean parseJson = true;

	public class KinesisRecord {

		Record record;

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
				String sequenceNumber = record.getSequenceNumber();
				String partitionKey = record.getPartitionKey();

				String approximateArrivalTime = arrivalFormatter
						.format(record.getApproximateArrivalTimestamp().toInstant());

				Headers headers = event.getHeaders();

				event.getHeaders().set("sequenceNumber", sequenceNumber);
				event.getHeaders().set("partitionKey", partitionKey);
				event.getHeaders().set("approximateArrivalTimestamp", approximateArrivalTime);
				event.getHeaders().set("streamName", kinesisConfig.getStreamName());
				headers.set("bridgeId", getId());
				getEventBus().notify(kr, event);

			});

			conditionalCheckpoint(processRecordsInput.getCheckpointer(), processRecordsInput);
		}

		@Override
		public void shutdown(ShutdownInput shutdownInput) {
			// TODO Auto-generated method stub

		}

	}

	public static class Builder {

		KinesisClientLibConfiguration kinesisConfig = null;
		EventBus eventBus;
		String arn;
		boolean parseJson = false;
		String appName;
		String streamName;
		String regionName;
		AWSCredentialsProvider credentialsProvider;
		Consumer<KinesisClientLibConfiguration> extraConfig;
		String workerId;
		long checkpointIntervalMillis = TimeUnit.SECONDS.toMillis(30);

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

		public Builder withStreamArn(String arn) {
			this.arn = arn;
			return this;
		}

		public Builder withCheckpointInterval(int v, TimeUnit unit) {
			this.checkpointIntervalMillis = unit.toMillis(v);
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
				kinesisConfig = new KinesisClientLibConfiguration(appName, streamName, credentialsProvider,
						workerId);
				if (regionName != null) {
					kinesisConfig.withRegionName(regionName);
				}
			}

			bridge.kinesisConfig = kinesisConfig;

			if (extraConfig != null) {
				extraConfig.accept(bridge.kinesisConfig);
			}
			bridge.eventBus = eventBus;

			bridge.arn = arn;
			AmazonKinesisAsyncClient asyncClient = new AmazonKinesisAsyncClient(
					kinesisConfig.getKinesisCredentialsProvider());

			if (kinesisConfig.getRegionName() != null) {
				asyncClient.setRegion(Region.getRegion(Regions.fromName(kinesisConfig.getRegionName())));
			}

			if (bridge.parseJson) {
				JsonParsingConsumer.apply(bridge);
			}

			logger.info("bridgeId  : {}", bridge.getId());
			logger.info("appName   : {}", kinesisConfig.getApplicationName());
			logger.info("streamName: {}", kinesisConfig.getStreamName());
			logger.info("regionName: {}", kinesisConfig.getRegionName());
			logger.info("workerId  : {}", kinesisConfig.getWorkerIdentifier());
			logger.info("streamArn : {}", bridge.arn);

			logger.info("created {} ... don't forget to call start()", bridge);
			return bridge;
		}
	}

	
	public void start() {
		logger.info("starting {}...", this);
		IRecordProcessorFactory factory = new IRecordProcessorFactory() {

			@Override
			public IRecordProcessor createProcessor() {
				BridgeRecordProcessor p = new BridgeRecordProcessor();
				logger.info("creating {}", p);
				return p;
			}
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

	}

	protected KinesisReactorBridge() {

	}

	public String getArn() {
		return getStreamArn();
	}

	public String getStreamArn() {
		if (arn == null) {
			throw new IllegalStateException("withStreamArn() must have been set");
		}
		return arn;
	}

	public Selector eventsFromBridgeSelector() {
		return Selectors.predicate(p -> {
			if (p instanceof KinesisRecord) {
				return ((KinesisRecord) p).getBridge() == this;
			}
			return false;
		});
	}

	public AmazonKinesisAsyncClient getKinesisAsyncClient() {
		return asyncKinesisClient;
	}

	protected void conditionalCheckpoint(IRecordProcessorCheckpointer checkpointer, ProcessRecordsInput input) {
		try {
			long millisSinceLastCheckpoint = Instant.now().toEpochMilli() - lastCheckpoint.get();
			if (millisSinceLastCheckpoint > checkpointIntervalMillis) {

				if (!input.getRecords().isEmpty()) {
					Record r = input.getRecords().get(input.getRecords().size() - 1);
					logger.info("checkpointing app {} for stream {} at {}", kinesisConfig.getApplicationName(),
							kinesisConfig.getStreamName(), r.getSequenceNumber());
					checkpointer.checkpoint(input.getRecords().get(input.getRecords().size() - 1));
				}
				lastCheckpoint.set(Instant.now().toEpochMilli());
			}

		} catch (InvalidStateException | ShutdownException e) {
			logger.warn("problem checkpointing", e);
		}
	}


}
