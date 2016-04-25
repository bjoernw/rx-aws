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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SNSMessageTest {
	ObjectMapper mapper = new ObjectMapper();
	String message = "{\n" + 
			"  \"Type\" : \"Notification\",\n" + 
			"  \"MessageId\" : \"abc2b453-349a-5637-b17c-fbc4aa639b6c\",\n" + 
			"  \"TopicArn\" : \"arn:aws:sns:us-east-1:550588888888:test\",\n" + 
			"  \"Subject\" : \"world\",\n" + 
			"  \"Message\" : \"hello\",\n" + 
			"  \"Timestamp\" : \"2016-04-25T04:27:37.504Z\",\n" + 
			"  \"SignatureVersion\" : \"1\",\n" + 
			"  \"Signature\" : \"SIGDATASIGDATA3Jee8fReP4hdnirjTyRy6TDk7lewTApqLnff882FCQMeDjr8XF3q4oHRcDCOYyy2eOHOafBJnSCPs0DgSJ3A3cNl9OeLeq4INg==\",\n" + 
			"  \"SigningCertURL\" : \"https://sns.us-east-1.amazonaws.com/SimpleNotificationService-bb750dd426d95ee93901aaaaaaaaaaaa.pem\",\n" + 
			"  \"UnsubscribeURL\" : \"https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:550534291128:test:d969ed7a-aaaa-aaaa-aaaa-aaaaaaaaaaaa\"\n" + 
			"}";
	
	
	@Test
	public void testIt() throws IOException {
		JsonNode n = mapper.readTree(message);
		SNSMessage m = new SNSMessage(n);

		Assertions.assertThat(m.getBody()).isEqualTo("hello");
		Assertions.assertThat(m.getMessageId()).contains("abc2b");
		Assertions.assertThat(m.getType()).isEqualTo("Notification");
		Assertions.assertThat(m.getBodyAsJson().isMissingNode()).isTrue();
	}
	
	@Test
	public void testValidJson() throws IOException {
		ObjectNode n = (ObjectNode) mapper.readTree(message);
		n.put("Message", mapper.createObjectNode().put("foo", "bar").toString());
		SNSMessage m = new SNSMessage(n);

		
		Assertions.assertThat(m.getBodyAsJson().isMissingNode()).isFalse();
		Assertions.assertThat(m.getBodyAsJson().path("foo").asText()).isEqualTo("bar");
	}
}
