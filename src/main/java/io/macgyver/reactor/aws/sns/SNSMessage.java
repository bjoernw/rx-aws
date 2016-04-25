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
import com.fasterxml.jackson.databind.node.MissingNode;

public class SNSMessage {

	static Logger logger = LoggerFactory.getLogger(SNSMessage.class);
	
	static ObjectMapper mapper = new ObjectMapper();

	JsonNode snsEnvelope;

	JsonNode jsonBody;

	SNSMessage(JsonNode n) {
		this.snsEnvelope = n;
	}

	public synchronized JsonNode getBodyAsJson() {
		try {
			if (jsonBody != null) {
				return jsonBody;
			}
			jsonBody = mapper.readTree(getBody());

			return jsonBody;
		} catch (IOException e) {
			jsonBody = MissingNode.getInstance();
			logger.warn("problem parsing json: "+e.toString());
		}
		return jsonBody;
	}

	public String getBody() {
		return snsEnvelope.get("Message").asText();
	}

	public String getType() {
		return snsEnvelope.path("Type").asText(null);
	}
	public String getMessageId() {
		return snsEnvelope.path("MessageId").asText(null);
	}
	public String getSubject() {
		return snsEnvelope.path("Subject").asText(null);
	}
	public String getTopicArn() {
		return snsEnvelope.path("TopicArn").asText(null);
	}
	public String getTimestamp() {
		return snsEnvelope.path("Timestamp").asText(null);
	}
	public String getSignatureVersion() {
		return snsEnvelope.path("SignatureVersion").asText(null);
	}
	public String getSignature() {
		return snsEnvelope.path("Signature").asText(null);
	}
	public String getSigningCertUrl() {
		return snsEnvelope.path("SigningCertURL").asText(null);
	}
	public String getUnsubscribeURL() {
		return snsEnvelope.path("UnsubscribeURL").asText(null);
	}
}
