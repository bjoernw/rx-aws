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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.macgyver.reactor.aws.util.MoreSelectors;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class SNSSelectors {

	static Logger logger = LoggerFactory.getLogger(SNSSelectors.class);
	static ObjectMapper mapper = new ObjectMapper();

	

	public static Selector snsTopicSelector(String name) {
		return MoreSelectors.typedPredicate((SNSMessage m) -> {		
			return m.getTopicArn().endsWith(":"+name);
		});
	}
	
	public static Selector snsTopicArnSelector(String arn) {
		return MoreSelectors.typedPredicate((SNSMessage m) -> {		
			return m.getTopicArn().equals(arn);
		});
	}
	
	public static Selector anySNSMessageSelector() {
		return Selectors.type(SNSMessage.class);
	}


	
}
