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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.ImmutableList;

import io.macgyver.reactor.aws.sqs.SQSReactorBridge;
import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;

public class SNSTest {

	
	public static final void main(String [] args) throws InterruptedException  {
		
		EventBus bus = EventBus.create(Environment.initializeIfEmpty());
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
		DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();
		
		AmazonSQSClient client = new AmazonSQSClient(chain);
		
		//client.createQueue("test");
		String x = "arn:aws:sqs:us-east-1:550534291128:test";
		
		AmazonSNSClient sns = new AmazonSNSClient(chain);
		//CreateTopicResult ctr = sns.createTopic("test");
		String topicArn = "arn:aws:sns:us-east-1:550534291128:test";
		
		SQSReactorBridge bridge = new SQSReactorBridge.Builder().withSNSSupport(true).withEventBus(bus).withQueueName("test").withCredentialsProvider(chain).build().start();
		
		bus.on(SNSSelectors.snsTopicSelector("test"), (Event<SNSMessage> c)-> {
			System.out.println(">> "+c.getData().getSubject());
			System.out.println(c.getData().getTopicArn());
			System.out.println(c.getData().getBodyAsJson());
		});
		
		
		sns.publish(topicArn, "{\"foo\":\"bar\"}", "world");
		System.out.println(topicArn);
		
		
	
		Thread.sleep(30000L);
	}
}
