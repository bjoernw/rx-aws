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
package io.macgyver.reactor.aws.kinesis;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;

import io.macgyver.reactor.aws.kinesis.KinesisReactorBridge.KinesisRecord;
import io.macgyver.reactor.aws.util.EventUtil;
import reactor.bus.Event;
import reactor.fn.Consumer;

public class JsonParsingConsumer implements Consumer<Event<KinesisReactorBridge.KinesisRecord>> {

	private static final Logger logger = LoggerFactory.getLogger(JsonParsingConsumer.class);
	private ObjectMapper mapper = new ObjectMapper();

	@Override
	public void accept(Event<KinesisReactorBridge.KinesisRecord> t) {

		try {
			KinesisRecord record = t.getData();
			JsonNode n = mapper.readTree(new ByteBufferBackedInputStream(record.getRecord().getData()));
			Event<JsonNode> event = Event.wrap(n);
			EventUtil.copyEventHeaders(t, event);
			record.getBridge().getEventBus().notify(n, event);
		} catch (IOException | RuntimeException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("could not parse json", e);
			} else {
				logger.warn("could not parse json: " + e.getMessage());
			}
		}
	}

	public static void apply(KinesisReactorBridge bridge) {
		bridge.getEventBus().on(bridge.eventsFromBridgeSelector(), new JsonParsingConsumer());
	}

}
