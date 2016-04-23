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

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import ch.qos.logback.core.boolex.Matcher;
import reactor.Environment;
import reactor.bus.EventBus;

public abstract class AbstractSQSIntegrationTest {

	static Logger logger = LoggerFactory.getLogger(AbstractSQSIntegrationTest.class);
	static AmazonSQSAsyncClient client;
	static String url;
	static String queueName = "junit-" + AbstractSQSIntegrationTest.class.getName().replace(".", "-") + "-"
			+ System.currentTimeMillis();
	static EventBus bus;

	@AfterClass
	public static void cleanup() {
		if (url != null && client != null) {
			logger.info("deleting queue: {}", url);
			try {
				client.deleteQueue(url);
			} catch (RuntimeException e) {
				logger.warn("problem deleting queue", e);
			}

			
			if (client != null) {
				ListQueuesResult r = client.listQueues();
				r.getQueueUrls().forEach(x -> {
					cleanupAbandonedTestQueue(x);
				});
			}
		}
	}

	private static void cleanupAbandonedTestQueue(String url) {
	
		long maxAge=5;
		Pattern p = Pattern.compile(".*junit.*SQSIntegrationTest-(\\d+)");
		java.util.regex.Matcher m = p.matcher(url);
		if (m.matches())  {
			try {
				long createTime = Long.parseLong(m.group(1));
				long ageInMinutes = TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - createTime);
				if (ageInMinutes>maxAge) {
					logger.info("deleting {} because it is older than {} minutes",url,maxAge);
					client.deleteQueue(url);
				}
				else {
					logger.info("not deleting {}",url);
				}
			
			} catch(QueueDoesNotExistException e) {
				// eventual consistency...not a problem
			}catch (RuntimeException e) {
			
				logger.warn("problem deleting queue: " + url, e);
			}
		}
		
	}

	@BeforeClass
	public static void setup() {
		try {
			client = new AmazonSQSAsyncClient();
			client.setRegion(Region.getRegion(Regions.US_WEST_1));

			url = client.getQueueUrl(queueName).getQueueUrl();
			logger.info("using url: " + url);
		} catch (QueueDoesNotExistException e) {
			logger.info("queue does not exist");
			CreateQueueResult r = client.createQueue(queueName);

			try {
				Thread.sleep(5000L);
			} catch (InterruptedException ex) {
			}
			url = r.getQueueUrl();

		} catch (Exception e) {
			logger.error("", e);

		}
		bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);

		Assume.assumeTrue(client != null && url != null && bus != null);
	}

	public AmazonSQSAsyncClient getSQSClient() {
		return client;
	}

	public String getQueueUrl() {
		return url;
	}

	public void emptyQueue() {

		ReceiveMessageResult r = getSQSClient().receiveMessage(getQueueUrl());

		while (r.getMessages().size() > 0) {
			r.getMessages().forEach(it -> {
				getSQSClient().deleteMessage(getQueueUrl(), it.getReceiptHandle());
			});

			r = getSQSClient().receiveMessage(getQueueUrl());
		}
		logger.info("queue is empty: {}", getQueueUrl());
	}

	public EventBus getEventBus() {
		return bus;
	}
}
