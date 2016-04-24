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
package io.macgyver.reactor.aws.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.macgyver.reactor.aws.kinesis.KinesisReactorBridge.KinesisRecord;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class KinesisRecordSelectors {

	static Logger logger = LoggerFactory.getLogger(KinesisRecordSelectors.class);

	public static Selector anyKinesisRecord() {
		return Selectors.type(KinesisRecord.class);
	}

	public static Selector streamArn(String arn) {
		return Selectors.predicate(it -> {

			if (it instanceof KinesisRecord) {
				KinesisRecord kr = (KinesisRecord) it;

				return arn.equals(kr.getStreamArn());

			}
			return false;

		});
	}

	public static Selector streamName(String name) {
		return Selectors.predicate(it -> {

			if (it instanceof KinesisRecord) {
				KinesisRecord kr = (KinesisRecord) it;

				return kr.getStreamName().equals(name);

			}
			return false;

		});
	}
}
