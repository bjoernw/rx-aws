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
package io.macgyver.reactor.aws.sns;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.macgyver.reactor.aws.sqs.SQSMessage;
import io.macgyver.reactor.aws.sqs.SQSReactorBridge;
import io.macgyver.reactor.aws.util.MoreSelectors;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.fn.Consumer;

public class SNSAdapter implements Consumer<Event<SQSMessage>> {

	static Logger logger = LoggerFactory.getLogger(SNSAdapter.class);
	static ObjectMapper mapper = new ObjectMapper();
	EventBus bus;

	public SNSAdapter(EventBus bus) {
		this.bus = bus;
	}
	
	@Override
	public void accept(Event<SQSMessage> event) {

		try {
			JsonNode n = mapper.readTree(event.getData().getMessage().getBody());
			if (n.path("Type").asText().equals("Notification") && n.has("TopicArn")) {
				SNSMessage snsMessage = new SNSMessage(n);
				Event<SNSMessage> newEvent = Event.wrap(snsMessage);
				bus.notify(snsMessage, newEvent);
			}

		} catch (IOException e) {
			logger.warn("could not parse message: " + e.toString());
		}
	}
	public static void applySNSAdapter(SQSReactorBridge bridge, EventBus bus) {
		Selector selector = MoreSelectors.typedPredicate((SQSMessage m) -> {
			try {
				
				if (m.getBridge()!=bridge) {
					return false;
				}
				String s = m.getMessage().getBody();
				JsonNode n = mapper.readTree(s);

				boolean b = n.path("Type").asText().equals("Notification");
				
				return b;
			} catch (IOException e) {
				logger.warn("could not parse message: " + e.toString());
			}

			return false;
		});
		
		Consumer consumer = new SNSAdapter(bus);
		
		bus.on(selector,consumer);
	}
}