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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.amazonaws.services.sqs.model.Message;

import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class SQSMessageSelectorsTest {

	@Test
	public void testArn() {
		
		Selector s = SQSMessageSelectors.arn("foo:bar");
		
		Assertions.assertThat(s).isNotNull();
		
		Assertions.assertThat(Selectors.$("test").getObject()).isEqualTo("test");

		
		Assertions.assertThat(s.matches("foo")).isFalse();
		
		Assertions.assertThat(s.matches("foo:bar")).isFalse();
		
		Message m = new Message();
		SQSReactorBridge b = new SQSReactorBridge();
		
		SQSMessage sqsMessage = new SQSMessage(b,m);
		
		Assertions.assertThat(s.matches(sqsMessage)).isFalse();
		
		Assertions.assertThat(sqsMessage.getMessage().getAttributes().get("arn")).isNull();
		
		m.getAttributes().put("arn", "baz:baz");
		
		Assertions.assertThat(sqsMessage.getMessage().getAttributes().get("arn")).isEqualTo("baz:baz");
		
	}
	
	@Test
	public void testDate() {
		
		
	}
}
