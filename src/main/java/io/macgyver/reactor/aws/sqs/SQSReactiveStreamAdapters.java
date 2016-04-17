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

import org.reactivestreams.Publisher;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.JsonNode;

import io.macgyver.reactor.aws.sqs.SQSReactorBridge.SQSMessage;
import io.macgyver.reactor.aws.util.RxJson;
import reactor.bus.Event;
import rx.Observable;
import rx.RxReactiveStreams;

public class SQSReactiveStreamAdapters {

	
	public static  Observable<Event<SQSMessage>> toObservableEvent(Publisher<? extends Event<?>> publisher) {
		return (Observable<Event<SQSMessage>>) RxReactiveStreams.toObservable(publisher);
	}
	
	public static  Observable<SQSMessage> toObservableSQSMessage(Publisher<? extends Event<?>> publisher) {
		return toObservableEvent(publisher).map(event -> event.getData());
	}
	
	public static  Observable<Message> toObservableMessage(Publisher<? extends Event<?>> publisher) {
		return  toObservableSQSMessage(publisher).map(sqs -> sqs.getMessage());
	}
	
	public static  Observable<String> toObservablString(Publisher<? extends Event<?>> publisher) {
		return toObservableMessage(publisher).map(m->m.getBody());
	}
	
	public static Observable<JsonNode> toObservableJsonNode(Publisher<? extends Event<?>> publisher) {
		return toObservablString(publisher).flatMap(RxJson.STRING_TO_JSON);
	}
}
