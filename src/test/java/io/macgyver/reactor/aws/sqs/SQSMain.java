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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.fasterxml.jackson.databind.JsonNode;

import io.macgyver.reactor.aws.sqs.SQSReactorBridge.SQSMessage;
import io.macgyver.reactor.aws.util.MoreSelectors;
import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.fn.Predicate;
import rx.Observable;

public class SQSMain {
	public static void main(String[] args) throws Exception {

		
		EventBus bus = EventBus.create(Environment.newDispatcher());
		
		
		
		DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();
		String url = System.getProperty("sqs.url");
		AmazonSQSAsyncClient x = new AmazonSQSAsyncClient(chain);

	
		x.sendMessage(url, "{\"name\":\"Rob\"}");
		
		Predicate<SQSMessage> p = msg->{
			
			return msg.getUrl().endsWith("test");
		
		};
		
		
		bus.on(MoreSelectors.typedPredicate((SQSMessage msg)-> msg.getUrl().endsWith("/test")), it -> System.out.println(it));
		
		SQSReactorBridge c =
				new SQSReactorBridge.Builder()
				.withUrl(url)
				.withEventBus(bus)
				.withJsonParsing(true)
				.build()
				.start();

	
	//	RxEventBus.observe(bus, Selectors.T(String.class),String.class).map(s->"hello "+s).subscribe(it->System.out.println(it));
		
		
		
	//	bus.on(SQSMessageSelectors.anySQSMessage(),consumer -> {
		//	System.out.println(consumer);
		//});
		
		
		Thread.sleep(90000);
	}
}
