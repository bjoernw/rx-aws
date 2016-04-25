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
package io.macgyver.reactor.aws.sqs;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.collect.Lists;

import io.macgyver.reactor.aws.sqs.SQSReactorBridge.SQSMessage;
import reactor.bus.Event;
import reactor.bus.selector.Selectors;

public class SQSReactorBridgeIntegrationTest extends AbstractSQSIntegrationTest {

	@Test
	public void testIt() throws InterruptedException{

	
		emptyQueue();

		SQSReactorBridge b = new SQSReactorBridge.Builder().withSQSClient(getSQSClient()).withEventBus(getEventBus())
				.withUrl(getQueueUrl()).build().start();

		Assertions.assertThat(b.getQueueArn()).startsWith("arn:aws:sqs:");
		CountDownLatch latch = new CountDownLatch(3);
		List<Event<SQSMessage>> list = Lists.newCopyOnWriteArrayList();
		
		bus.on(Selectors.T(SQSMessage.class), (Event<SQSMessage> evt) -> {
			logger.info("Received: {}",evt);
			list.add(evt);
			latch.countDown();
			
		});

		getSQSClient().sendMessage(getQueueUrl(), "test1");
		getSQSClient().sendMessage(getQueueUrl(), "test2");
		getSQSClient().sendMessage(getQueueUrl(), "test3");
		Assertions.assertThat(latch.await(20,TimeUnit.SECONDS)).isTrue();
		logger.info("received all");
		list.forEach(evt->{
			Assertions.assertThat(evt.getHeaders().get("arn").toString()).startsWith("arn:aws:sqs:");
			Assertions.assertThat(evt.getHeaders().get("bridgeId").toString()).isEqualTo(b.getId());
			Assertions.assertThat(evt.getHeaders().get("url").toString()).isEqualTo(getQueueUrl());
			
			SQSMessage msg = evt.getData();
			Assertions.assertThat(msg).isNotNull();
			Assertions.assertThat(msg.getUrl()).isEqualTo(evt.getHeaders().get("url"));
			Assertions.assertThat(msg.getBridge()).isSameAs(b);
			Assertions.assertThat(msg.getArn()).isEqualTo(evt.getHeaders().get("arn"));
			
			Message sm = msg.getMessage();
			
			Assertions.assertThat(sm.getAttributes()).containsEntry("url", msg.getUrl()).containsEntry("arn", msg.getArn()).containsEntry("bridgeId", b.getId());
			
			
			
		});
	
	}
}
