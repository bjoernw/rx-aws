/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.macgyver.reactor.aws.sqs;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.macgyver.reactor.aws.AbstractReactorBridge;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class SQSReactorBridge extends AbstractReactorBridge {

	static Logger logger = LoggerFactory.getLogger(SQSReactorBridge.class);

	static ScheduledExecutorService globalExecutor = Executors.newScheduledThreadPool(1,
			new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SQSBridge-scheduler-%s").build());

	protected AtomicBoolean running = new AtomicBoolean(false);

	protected String arn;

	public class SQSMessage {

		Message message;

		public SQSMessage(Message m) {
			this.message = m;

		}

		public SQSReactorBridge getBridge() {
			return SQSReactorBridge.this;
		}

		public Message getMessage() {
			return message;
		}

		public String getUrl() {
			String url = message.getAttributes().get("url");
			return url;
		}

		public String getArn() {
			String arn = message.getAttributes().get("arn");
			return arn;
		}
	}

	protected SQSReactorBridge() {
		// TODO Auto-generated constructor stub
	}

	public String getArn() {
		return arn;
	}

	void dispatch(Message m) {

		if (eventBus == null) {
			logger.warn("EventBus not set...message will be discarded");
			deleteMessageIfNecessary(m);
		} else {

			if (logger.isDebugEnabled()) {
				logger.debug("dispatching on {}: {} for {}", eventBus, m);
			}
			SQSMessage sm = new SQSMessage(m);

			Event<SQSMessage> em = Event.wrap(sm);

			String arn = getArn();
			if (arn != null) {
				m.getAttributes().put("arn", arn);
			}
			m.getAttributes().put("url", getUrl());
			m.getAttributes().put("bridgeId", getId());

			m.getAttributes().forEach((k, v) -> {
				em.getHeaders().set(k, v);
			});

			eventBus.notify(sm, em);

			deleteMessageIfNecessary(em);

		}

	}

	protected void deleteMessageIfNecessary(Event<SQSMessage> event) {
		deleteMessageIfNecessary(event.getData().getMessage());
	}

	protected void deleteMessageIfNecessary(Message m) {
		if (isAutoDeleteEnabled()) {
			if (logger.isDebugEnabled()) {
				logger.debug("deleting message: {}", m.getReceiptHandle());
				client.deleteMessageAsync(getUrl(), m.getReceiptHandle());
			}
		}
	}

	AmazonSQSAsyncClient client;
	AtomicLong failureCount = new AtomicLong();
	EventBus eventBus;
	int waitTimeSeconds;
	boolean autoDeleteEnabled = true;
	ScheduledExecutorService scheduledExecutorService;

	Supplier<String> urlSupplier = new SQSUrlSupplier(null);

	public String getUrl() {
		return urlSupplier.get();
	}

	public AmazonSQSAsyncClient getAsyncClient() {
		return client;
	}

	public AtomicLong getFailureCount() {
		return failureCount;
	}

	public EventBus getEventBus() {
		return eventBus;
	}

	public boolean isAutoDeleteEnabled() {
		return autoDeleteEnabled;
	}

	public static class SQSUrlSupplier implements Supplier<String> {

		String queueUrl;

		AmazonSQSAsyncClient asyncClient;
		String queueName;

		public SQSUrlSupplier(String url) {
			this.queueUrl = url;
		}

		public SQSUrlSupplier(AmazonSQSAsyncClient client, String queueName) {
			this.asyncClient = client;
			this.queueName = queueName;
		}

		@Override
		public String get() {
			if (queueUrl != null) {
				return queueUrl;
			} else if (asyncClient != null && queueName != null) {
				String url = asyncClient.getQueueUrl(queueName).getQueueUrl();
				return url;
			}

			throw new IllegalArgumentException("must provide explicit url or a cliient+queueName");
		}

	}

	public static class Builder {

		static Pattern urlToArnPattern = Pattern.compile("https://sqs\\.(.*)\\.amazonaws\\.com/(.*)/(.*)");

		String url;
		AmazonSQSAsyncClient client;
		AWSCredentialsProvider credentialsProvider;
		EventBus eventBus;
		int waitTimeSeconds = 10;
		ScheduledExecutorService executor;
		String queueName;
		String arn;
		boolean jsonParsing = false;
		Region region;

		public Builder withRegion(Regions region) {
			return withRegion(Region.getRegion(region));
		}

		public Builder withRegion(Region region) {
			this.region = region;
			return this;
		}

		public Builder withRegion(String region) {
			return withRegion(Regions.fromName(region));
		}

		public Builder withEventBus(EventBus eventBus) {
			this.eventBus = eventBus;
			return this;
		}

		public Builder withJsonParsing(boolean b) {
			this.jsonParsing = b;
			return this;
		}

		public Builder withScheduledExecutorService(ScheduledExecutorService s) {
			this.executor = s;
			return this;
		}

		public Builder withWaitTimeSeconds(int s) {
			this.waitTimeSeconds = s;
			return this;
		}

		public Builder withUrl(String url) {
			this.url = url;
			return this;
		}

		public Builder withArn(String arn) {
			this.arn = arn;
			return this;
		}

		public Builder withQueueName(String queueName) {
			this.queueName = queueName;
			return this;
		}

		public Builder withSQSClient(AmazonSQSAsyncClient client) {

			this.client = client;
			return this;
		}

		public Builder withCredentialsProvider(AWSCredentialsProvider p) {
			this.credentialsProvider = p;
			return this;
		}

		/**
		 * This should work for now. It is sketchy though. Going through a
		 * reverse
		 * proxy will cause it to fail.
		 * 
		 * @param url
		 * @return
		 */
		protected String urlToArn(String url) {

			if (url != null) {
				Matcher m = urlToArnPattern.matcher(url);
				if (m.matches()) {
					String arn = "arn:aws:sqs:" + m.group(1) + ":" + m.group(2) + ":" + m.group(3);

					return arn;
				}
			}
			logger.info("could not derive arn from URL: {}", url);
			return null;
		}

		public SQSReactorBridge build() {
			SQSReactorBridge c = new SQSReactorBridge();
			Preconditions.checkArgument(eventBus != null, "EventBus not set");

			c.eventBus = eventBus;
			if (client != null) {
				c.client = client;
			} else {
				if (credentialsProvider == null) {
					credentialsProvider = new DefaultAWSCredentialsProviderChain();
				}
				c.client = new AmazonSQSAsyncClient(credentialsProvider);
			}
			if (region != null) {
				c.client.setRegion(region);
			}
			if (url != null) {
				c.urlSupplier = Suppliers.memoize(new SQSUrlSupplier(url));
			} else {
				Preconditions.checkArgument(queueName!=null,"queue name must be specified if url is not specified");
				c.urlSupplier = Suppliers.memoize(new SQSUrlSupplier(c.client, queueName));
			}
			if (waitTimeSeconds > 0) {
				c.waitTimeSeconds = waitTimeSeconds;
			} else {
				c.waitTimeSeconds = 0;
			}
			c.scheduledExecutorService = executor != null ? executor : globalExecutor;

			if (jsonParsing) {
				SQSJsonParsingConsumer.apply(c);
			}
			if (arn != null) {
				c.arn = arn;
			} else {
				try {
					String url = c.getUrl();
					c.arn = urlToArn(url);
				} catch (RuntimeException e) {
					logger.info("could not obtain queue url", e);
				}
			}
			logger.info("constructed {}. Don't forget to call start()", c);
			return c;
		}
	}

	protected long calculateRescheduleDelayForException(Exception e) {
		long rescheduleDelay = Math.min(60000, 1000 * failureCount.get() * 3);

		// we may want to dial things back depending on the error
		return rescheduleDelay;

	}

	private class Handler implements AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult> {

		Handler() {

		}

		@Override
		public void onError(Exception exception) {
			long fc = failureCount.incrementAndGet();

			AtomicInteger chainCount = new AtomicInteger(0);
			Throwables.getCausalChain(exception).forEach(it -> {

				logger.warn("chain[{}]: {}", chainCount.getAndIncrement(), it.toString());

			});

		}

		@Override
		public void onSuccess(ReceiveMessageRequest request, ReceiveMessageResult result) {
			try {
				List<Message> list = result.getMessages();

				logger.debug("received {} messages from {}", (list != null) ? list.size() : 0, getUrl());
				SQSReactorBridge.this.failureCount.set(0);

				result.getMessages().forEach(m -> {
					try {
						SQSReactorBridge.this.dispatch(m);
					} catch (Exception e) {
						logger.error("could not dispatch event to reactor EventBus: {}", m);
					}
				});
			} finally {

			}
		}

	}

	public boolean isRunning() {
		return running.get();
	}

	public SQSReactorBridge start() {
		boolean oldValue = running.getAndSet(true);
		if (oldValue) {
			throw new IllegalStateException("already started");
		}
		logger.info("starting {}...", this);

		Runnable r = new Runnable() {

			@Override
			public void run() {
				while (isRunning()) {
					try {
						ReceiveMessageRequest request = new ReceiveMessageRequest();
						request.setQueueUrl(getUrl());
						request.setAttributeNames(ImmutableList.of("ALL"));
						request.setWaitTimeSeconds(waitTimeSeconds);
						Future<ReceiveMessageResult> result = client.receiveMessageAsync(request, new Handler());

						result.get(); // go ahead and block

					} catch (Exception e) {
						logger.warn("", e);
						failureCount.incrementAndGet();

					}
					try {
						long rescheduleDelay = Math.min(60000, 1000 * failureCount.get() * 3);
						if (rescheduleDelay > 0) {
							logger.info("pausing for {}ms due to errors", rescheduleDelay);
							Thread.sleep(rescheduleDelay);
						}

					} catch (InterruptedException e) {
						// swallow it
					}
				}

			}

		};
		Thread t = new Thread(r);
		t.setDaemon(true);
		t.start();

		return this;
	}

	public String toString() {
		String url=null;
		try {
			url = getUrl();
		}
		catch (RuntimeException e) {
			// swallow it...this is toString()
		}
		return MoreObjects.toStringHelper(this).add("url", url).toString();
	}

	public Selector eventsFromBridgeSelector() {
		return Selectors.predicate(p -> {
			if (p instanceof SQSMessage) {
				return ((SQSMessage) p).getBridge() == this;

			}
			return false;
		});
	}
}
