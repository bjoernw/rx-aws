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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;

public class TimeIntervalCheckpointStrategy implements CheckpointStrategy {

	private static final Logger logger = LoggerFactory.getLogger(TimeIntervalCheckpointStrategy.class);
	public static final long DEFAULT_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(30);
	private final AtomicLong lastCheckpoint = new AtomicLong(0);
	private long checkpointIntervalMillis = 30000;

	@Override
	public Boolean call(Record record) {

		long millisSinceLastCheckpoint = System.currentTimeMillis() - lastCheckpoint.get();
		if (millisSinceLastCheckpoint > checkpointIntervalMillis) {
			lastCheckpoint.set(System.currentTimeMillis());
			return true;
		}
		return false;
	}

	public TimeIntervalCheckpointStrategy withCheckpointInterval(long time, TimeUnit unit) {
		checkpointIntervalMillis = unit.toMillis(time);
		return this;
	}

}
