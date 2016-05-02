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

import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class SQSMessageSelectors {

	public static Selector anySQSMessage() {

		return Selectors.type(SQSMessage.class);

	}

	public static Selector arn(String arn) {
		return Selectors.predicate(it -> {

			if (it instanceof SQSMessage) {
				String arnx = ((SQSMessage) it).getMessage().getAttributes().getOrDefault("arn", null);
				if (arnx != null && arnx.equals(arn)) {
					return true;
				}
			}
			return false;

		});
	}

	public static Selector queueName(String name) {
		return Selectors.predicate(it -> {
			if (it instanceof SQSMessage) {
				return ((SQSMessage) it).getUrl().endsWith("/" + name);
			}

			return false;
		});
	}
}
