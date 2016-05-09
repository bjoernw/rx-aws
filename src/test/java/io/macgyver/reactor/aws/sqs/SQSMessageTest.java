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

import java.util.concurrent.CountDownLatch;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selectors;

public class SQSMessageTest {

	@Test
	public void testIt() throws InterruptedException {
		Message m = new Message();
		
		EventBus b = EventBus.create(Environment.initializeIfEmpty());
		SQSReactorBridge bridge = new SQSReactorBridge.Builder()
				.withRegion("us-west-1")
				.withUrl("https://api.example.com")
				.withEventBus(b)
				.build();

		SQSMessage msg = new SQSMessage(bridge,m);

		Assertions.assertThat(msg.getMessage()).isSameAs(m);
	
	
		Event<SQSMessage> em = Event.wrap(msg);

		Assertions.assertThat(em.getData()).isNotNull();

		CountDownLatch latch = new CountDownLatch(1);

		Registration reg = b.on(Selectors.$("foo"), it -> {
			latch.countDown();
		});
		b.notify("foo", em);

		latch.await();

	}
}
