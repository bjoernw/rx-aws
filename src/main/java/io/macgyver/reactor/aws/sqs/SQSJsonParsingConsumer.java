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

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.macgyver.reactor.aws.kinesis.JsonParsingConsumer;
import io.macgyver.reactor.aws.kinesis.KinesisReactorBridge.KinesisRecord;
import io.macgyver.reactor.aws.sqs.SQSReactorBridge.SQSMessage;
import io.macgyver.reactor.aws.util.EventUtil;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.bus.Event.Headers;
import reactor.fn.Consumer;

public class SQSJsonParsingConsumer implements Consumer<Event<SQSMessage>> {
	Logger logger = LoggerFactory.getLogger(SQSJsonParsingConsumer.class);

	ObjectMapper m = new ObjectMapper();

	public SQSJsonParsingConsumer() {

	}

	public static void apply(SQSReactorBridge bridge) {
		bridge.getEventBus().on(bridge.eventsFromBridgeSelector(), new SQSJsonParsingConsumer());
	}

	
	protected Optional<Event<JsonNode>> transform(Event<SQSMessage> em) {
		try {

			JsonNode n = m.readTree(em.getData().getMessage().getBody());

			Event<JsonNode> event = Event.wrap(n);

			EventUtil.copyEventHeaders(em, event);
			return Optional.of(event);

		} catch (IOException | RuntimeException e) {

			if (logger.isDebugEnabled()) {
				logger.debug("failed to parse payload", e);
			} else {
				logger.warn("failed to parse payload: {}", e.toString());
			}
		}
		return Optional.empty();
	}



	@Override
	public void accept(Event<SQSMessage> t) {
		SQSMessage sqsMessage = t.getData();

		Optional<Event<JsonNode>> n = transform(t);
		n.ifPresent(it -> {
			sqsMessage.getBridge().getEventBus().notify(it.getData(), it);
		});

	}

}
