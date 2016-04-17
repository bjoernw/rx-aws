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

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.fasterxml.jackson.databind.JsonNode;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;

public class KinesisMain {

	public static final void main(String[] args) throws Exception {

		EventBus bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);

		bus.on(Selectors.matchAll(),e-> {
			System.out.println(e.getData());
		});
		bus.notify("greeting",Event.wrap("Hello, World!"));
		
		
		bus.on(Selectors.type(JsonNode.class), (Event<JsonNode> x) -> {
			System.out.println(x.getData());
		});

		new KinesisReactorBridge.Builder()
				.withRegion(Region.getRegion(Regions.US_WEST_1))
				.withStreamName("rx")
				.withAppName("foo")
				.withEventBus(bus)
				.withJsonParsing(true)
				.withStreamArn("foo")
				.build()
				.start();

	}
}
