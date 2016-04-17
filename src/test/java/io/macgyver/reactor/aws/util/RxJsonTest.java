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
package io.macgyver.reactor.aws.util;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

import rx.Observable;

public class RxJsonTest {

	
	@Test
	public void testIt() {
		JsonNode n = Observable.just("{\"name\":\"Rob\"}").flatMap(RxJson.STRING_TO_JSON).toBlocking().first();
		
		Assertions.assertThat(n.path("name").asText()).isEqualTo("Rob");
		
		 Assertions.assertThat(Observable.just("{\"name\":\"Rob\"").flatMap(RxJson.STRING_TO_JSON).toList().toBlocking().first()).hasSize(0);
			
			
		
	}
}
