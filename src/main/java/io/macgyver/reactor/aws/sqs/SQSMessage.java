package io.macgyver.reactor.aws.sqs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;

public class SQSMessage {

	static Logger logger = LoggerFactory.getLogger(SQSMessage.class);
	
	SQSReactorBridge bridge;
	
	Message message;

	AtomicReference<JsonNode> json = new AtomicReference<>(null);
	
	public SQSMessage(SQSReactorBridge bridge, Message m) {
		this.bridge = bridge;
		this.message = m;

	}

	public SQSReactorBridge getBridge() {
		return bridge;
	}

	public Message getMessage() {
		return message;
	}
	
	public String getBodyAsString() {
		return getMessage().getBody();
	}
	public JsonNode getBodyAsJson() {
		
		
		// There may be a race condition here if two threads call simultaneously, but it is not really a concern
		// Multiple instances of the body are not a problem.
		JsonNode jsonBody = json.get();
		if (jsonBody==null) {
			try {
				jsonBody = SQSReactorBridge.mapper.readTree(getBodyAsString());
				json.set(jsonBody);
			}
			catch (IOException | RuntimeException e) {
				logger.warn("problem parsing json body: "+e.toString());
				jsonBody = MissingNode.getInstance();
				json.set(jsonBody);
			}
		}
		return jsonBody;
	}
	public String getUrl() {
		return getBridge().getQueueUrl();
	}

	public String getArn() {
		return getBridge().getQueueArn();
	}
}