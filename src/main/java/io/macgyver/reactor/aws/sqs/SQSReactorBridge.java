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
package io.macgyver.reactor.aws.sqs;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.macgyver.reactor.aws.AbstractReactorBridge;
import io.macgyver.reactor.aws.sns.SNSAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class SQSReactorBridge extends AbstractReactorBridge {

    private final static Logger logger = LoggerFactory.getLogger(SQSReactorBridge.class);

    private static ScheduledExecutorService globalExecutor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SQSBridge-scheduler-%s").build());

    private final AtomicBoolean running = new AtomicBoolean(false);

    static ObjectMapper mapper = new ObjectMapper();

    private Thread daemonThread;

    protected SQSReactorBridge() {
    }

    public String getQueueArn() {
        return arnSupplier.get();
    }

    void dispatch(Message m) {
        if (eventBus == null) {
            logger.warn("EventBus not set...message will be discarded");
            deleteMessageIfNecessary(m);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("dispatching on {}: {}", eventBus, m);
            }
            SQSMessage sm = new SQSMessage(this, m);
            Event<SQSMessage> em = Event.wrap(sm);
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
            }
            client.deleteMessageAsync(getQueueUrl(), m.getReceiptHandle());
        }
    }

    private AmazonSQSAsyncClient client;
    private final AtomicLong failureCount = new AtomicLong();
    private EventBus eventBus;
    int waitTimeSeconds;
    private boolean autoDeleteEnabled = true;
    private ScheduledExecutorService scheduledExecutorService;
    private int maxBatchSize;
    private int visibilityTimeout;

    private Supplier<String> urlSupplier = new SQSUrlSupplier(null);
    private Supplier<String> arnSupplier = null;

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public String getQueueUrl() {
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

    public static class SQSArnSupplier implements Supplier<String> {

        Supplier<String> urlSupplier;
        AmazonSQSAsyncClient client;

        public SQSArnSupplier(AmazonSQSAsyncClient client, Supplier<String> urlSupplier) {
            this.urlSupplier = urlSupplier;
            this.client = client;
        }

        @Override
        public String get() {
            GetQueueAttributesRequest request = new GetQueueAttributesRequest();
            request.setQueueUrl(urlSupplier.get());
            request.setAttributeNames(ImmutableList.of("QueueArn"));

            GetQueueAttributesResult result = client.getQueueAttributes(request);

            return result.getAttributes().get("QueueArn");
        }
    }

    public static class SQSUrlSupplier implements Supplier<String> {

        private String queueUrl;
        private AmazonSQSAsyncClient asyncClient;
        private String queueName;

        public SQSUrlSupplier(String url) {
            this.queueUrl = url;
        }

        public SQSUrlSupplier(AmazonSQSAsyncClient client, String queueName) {
            this.asyncClient = client;
            this.queueName = queueName;
        }

        @Override
        public String get() {
            if (!Strings.isNullOrEmpty(queueUrl)) {
                return queueUrl;
            } else if (asyncClient != null && queueName != null) {
                return asyncClient.getQueueUrl(queueName).getQueueUrl();
            }
            throw new IllegalArgumentException("must provide explicit url or a client+queueName");
        }
    }

    public static class Builder {

        static Pattern urlToArnPattern = Pattern.compile("https://sqs\\.(.*)\\.amazonaws\\.com/(.*)/(.*)");

        private String url;
        private AmazonSQSAsyncClient client;
        private AWSCredentialsProvider credentialsProvider;
        private EventBus eventBus;
        private int waitTimeSeconds = SQSDefaults.waitTime;
        private ScheduledExecutorService executor;
        private String queueName;
        private String arn;
        private int maxBatchSize = SQSDefaults.maxBatchSize;
        private int visibilityTimeout = SQSDefaults.visibilityTimeout;
        private Region region;
        private boolean sns = SQSDefaults.snsSupport;

        public Builder withRegion(Regions region) {
            return withRegion(Region.getRegion(region));
        }

        public Builder withSNSSupport(boolean b) {
            this.sns = b;
            return this;
        }

        public Builder withRegion(Region region) {
            this.region = region;
            return this;
        }

        public Builder withRegion(String region) {
            return withRegion(Regions.fromName(region));
        }

        public Builder withMaxBatchSize(int s) {
            this.maxBatchSize = s;
            return this;
        }

        /**
         * The time in seconds that the client has to remove the message after the ReceiveMessageRequest before SQS
         * makes the item visible again in the queue. (default: 30 seconds)
         * @param timeout
         * @return
         */
        public Builder withVisibilityTimeout(int timeout) {
            this.visibilityTimeout = timeout;
            return this;
        }

        public Builder withEventBus(EventBus eventBus) {
            this.eventBus = eventBus;
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

        private AmazonSQSAsyncClient defaultClient() {
            if (credentialsProvider == null) {
                credentialsProvider = new DefaultAWSCredentialsProviderChain();
            }
            return new AmazonSQSAsyncClient(credentialsProvider);
        }

        public SQSReactorBridge build() {
            SQSReactorBridge c = new SQSReactorBridge();
            Preconditions.checkArgument(eventBus != null, "EventBus not set");
            Preconditions.checkArgument(region != null, "Region not set");

            c.eventBus = eventBus;
            c.client.setRegion(region);
            if (sns) {
                SNSAdapter.applySNSAdapter(c, c.eventBus);
            }
            if (client != null) {
                c.client = client;
            } else {
                c.client = defaultClient();
            }

            if (!Strings.isNullOrEmpty(url)) {
                c.urlSupplier = Suppliers.memoize(new SQSUrlSupplier(url));
            } else {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(queueName),
                        "queue name must be specified if url is not specified");
                c.urlSupplier = Suppliers.memoize(new SQSUrlSupplier(c.client, queueName));
            }
            if (waitTimeSeconds > 0) {
                c.waitTimeSeconds = waitTimeSeconds;
            } else {
                c.waitTimeSeconds = 0;
            }
            c.scheduledExecutorService = executor != null ? executor : globalExecutor;
            c.maxBatchSize = maxBatchSize;
            c.visibilityTimeout = visibilityTimeout;
            c.arnSupplier = Suppliers.memoize(new SQSArnSupplier(c.client, c.urlSupplier));

            logger.info("constructed {}. Don't forget to call start()", c);
            return c;
        }
    }

    private long calculateRescheduleDelay() {
        // we may want to dial things back depending on the error
        return Math.min(60000, 1000 * failureCount.get() * 3);
    }

    private class Handler implements AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult> {

        Handler() {

        }

        @Override
        public void onError(Exception exception) {
            failureCount.incrementAndGet();

            AtomicInteger chainCount = new AtomicInteger(0);
            Throwables.getCausalChain(exception).forEach(it -> {
                logger.warn("chain[{}]: {}", chainCount.getAndIncrement(), it.toString());
            });
        }

        @Override
        public void onSuccess(ReceiveMessageRequest request, ReceiveMessageResult result) {
            try {
                List<Message> list = result.getMessages();

                logger.debug("received {} messages from {}", (list != null) ? list.size() : 0, getQueueUrl());
                SQSReactorBridge.this.failureCount.set(0);

                if (list == null || list.isEmpty()) {
                    return;
                }

                for (Message message : list) {
                    try {
                        dispatch(message);
                    } catch (Exception e) {
                        logger.error("could not dispatch event to reactor EventBus: {}", message);
                    }
                }
            } finally {

            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public void stop() {
        logger.info("stopping {}. Waiting for thread to die.", this);
        running.set(false);
        try {
            daemonThread.join();
            logger.info("stopped {}", this);
        } catch (InterruptedException e) {
            logger.warn("", e);
        }
    }

    public SQSReactorBridge start() {
        boolean oldValue = running.getAndSet(true);
        if (oldValue) {
            throw new IllegalStateException("already started");
        }
        logger.info("starting {}...", this);

        Runnable r = () -> {
            while (isRunning()) {
                try {
                    ReceiveMessageRequest request = new ReceiveMessageRequest();
                    request.setQueueUrl(getQueueUrl());
                    request.setAttributeNames(ImmutableList.of("ALL"));
                    request.setWaitTimeSeconds(waitTimeSeconds);
                    request.setMaxNumberOfMessages(maxBatchSize);
                    request.setVisibilityTimeout(visibilityTimeout);
                    Future<ReceiveMessageResult> result = client.receiveMessageAsync(request, new Handler());
                    result.get(); // go ahead and block
                } catch (Exception e) {
                    logger.warn("", e);
                    failureCount.incrementAndGet();
                }
                try {
                    long rescheduleDelay = calculateRescheduleDelay();
                    if (rescheduleDelay > 0) {
                        logger.info("pausing for {}ms due to errors", rescheduleDelay);
                        Thread.sleep(rescheduleDelay);
                    }
                } catch (InterruptedException e) {
                    // swallow it
                }
            }
        };
        daemonThread = new Thread(r);
        daemonThread.setDaemon(true);
        daemonThread.start();
        return this;
    }

    public String toString() {
        String url = null;
        try {
            url = getQueueUrl();
        } catch (RuntimeException e) {
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

    static class SQSDefaults {
        static int maxBatchSize = 1;
        static int visibilityTimeout = 30;
        static int waitTime = 10;
        static boolean snsSupport = false;
    }
}
