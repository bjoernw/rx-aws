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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.google.common.base.Strings;

public abstract class AbstractKinesisIntegrationTest {

	static Logger logger = LoggerFactory.getLogger(AbstractKinesisIntegrationTest.class);
	static String streamName = null;
	static boolean kinesisAvailable = false;
	static AmazonKinesisAsyncClient kinesisClient = null;

	public static AmazonKinesisAsyncClient getKinesisClient() {
		if (kinesisClient == null) {
			kinesisClient = new AmazonKinesisAsyncClient(new DefaultAWSCredentialsProviderChain());
		}
		return kinesisClient;
	}

	@Before
	public void checkAvailability() {
		Assume.assumeTrue(kinesisAvailable);
	}

	@BeforeClass
	public static void setup() {

		try {
			AmazonKinesisAsyncClient client = getKinesisClient();

			client.listStreams().getStreamNames().forEach(s -> {
				logger.info("existing stream: {}", s);

			});
			boolean streamAvailable = false;
			try {
				com.amazonaws.services.kinesis.model.DescribeStreamResult r = client.describeStream(getStreamName());

				streamAvailable = true;

			} catch (RuntimeException e) {
				logger.info("stream unavailable: " + e.toString());
				streamAvailable = false;
			}

			if (streamAvailable == false) {
				logger.info("creating stream: {}", getStreamName());
				client.createStream(getStreamName(), 1);
			}

			boolean done = false;
			long t0 = System.currentTimeMillis();
			do {
				com.amazonaws.services.kinesis.model.DescribeStreamResult result = client
						.describeStream(getStreamName());
				String status = result.getStreamDescription().getStreamStatus();
				logger.info("stream status: {}", status);
				if ("ACTIVE".equals(status)) {
					streamAvailable = true;
					logger.info("stream {} is AVAILABLE", streamName);
					done = true;
				}
				if ("DELETING".equals(status)) {
					streamAvailable = false;
					done = true;
				}
				if ("DELETED".equals(status)) {
					streamAvailable = false;
					done = true;
				}
				if ("CREATING".equals(status)) {
					logger.info("waiting for stream {} to become available...", getStreamName());
					try {
						Thread.sleep(5000L);
					} catch (Exception e) {
					}
					;
				}

				if (System.currentTimeMillis() > t0 + TimeUnit.MINUTES.toMillis(2)) {
					done = true;
					streamAvailable = false;
				}
			} while (!done);

			if (streamAvailable) {
				logger.info("stream is avaialbale: {}", getStreamName());
			} else {
				logger.info("stream is not available: {}", getStreamName());
				logger.info("kinesis integration tests will be disabled");
			}
			kinesisAvailable = streamAvailable;
			Assume.assumeTrue(streamAvailable);
		} catch (RuntimeException e) {

			logger.warn("could not get stream...integration tests will be disabled: " + e.toString());
			kinesisAvailable=false;
			
		}

		if (kinesisAvailable) {
			deleteAllOldTestStreams();
		}
		Assume.assumeTrue(kinesisAvailable);
	}

	public static void deleteAllOldTestStreams() {

		Pattern p = Pattern.compile("junit-.*KinesisIntegrationTest.*-(\\d+)");
		boolean hasMoreStreams = true;
		AtomicReference<String> ref = new AtomicReference<String>(null);
		while (hasMoreStreams) {
			ListStreamsRequest request = new ListStreamsRequest();
			if (ref.get() != null) {
				request.setExclusiveStartStreamName(ref.get());
			}
			com.amazonaws.services.kinesis.model.ListStreamsResult result = getKinesisClient().listStreams(request);
			result.getStreamNames().forEach(it -> {
				ref.set(it);
				Matcher m = p.matcher(it);

				if (getStreamName().equals(it)) {
					// ignore current stream
				} else if (m.matches()) {
					logger.info("test stream: {}", it);

					long ageInMinutes = TimeUnit.MILLISECONDS
							.toMinutes(Math.abs(System.currentTimeMillis() - Long.parseLong(m.group(1))));

					logger.info("stream is {} minutes old", ageInMinutes);
					if (ageInMinutes > 30) {
						logger.info("deleting {}", it);
						getKinesisClient().deleteStream(it);
					}

				} else {
					logger.info("not a test stream: {}", it);
				}

			});
			hasMoreStreams = result.isHasMoreStreams();
		}
		ListStreamsRequest request;

	}

	public static String getStreamName() {
		try {
			if (streamName != null) {
				return streamName;
			}
			File f = new File("./test-stream.properties");
			if (f.exists()) {
				byte[] b = Files.readAllBytes(f.toPath());
				Properties p = new Properties();
				p.load(new ByteArrayInputStream(b));
				String val = p.getProperty("streamName");
				if (!Strings.isNullOrEmpty(val)) {
					streamName = val;
					return streamName;
				}
			}

			String newStreamName = ("junit-" + AbstractKinesisIntegrationTest.class.getName() + "-"
					+ System.currentTimeMillis()).replace(".", "-");

			Properties p = new Properties();
			p.put("streamName", newStreamName);
			try (FileWriter fw = new FileWriter(f)) {
				p.store(fw, "");
			}
			streamName = newStreamName;
			return streamName;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
