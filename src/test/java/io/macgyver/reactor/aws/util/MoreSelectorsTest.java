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
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class MoreSelectorsTest {

	ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testJsonPredicate() {

		Selector<JsonNode> s = MoreSelectors.jsonPredicate(p -> {
			return p.path("name").asText().equals("Jerry Garcia");
		});

		// first check the "official" Selectors behavior
		Assertions.assertThat(Selectors.predicate(p -> {
			return true;
		}).getObject()).isNotNull();
		Assertions.assertThat(Selectors.predicate(p -> {
			return true;
		}).getHeaderResolver()).isNull();

		// Now make sure that we behave similarly
		Assertions.assertThat(s.getHeaderResolver()).isNull();
		Assertions.assertThat(s.getObject()).isNotNull();
		
		
		Assertions.assertThat(s.matches(mapper.createObjectNode().put("name", "Jerry Garcia"))).isTrue();
		Assertions.assertThat(s.matches(mapper.createObjectNode())).isFalse();
		Assertions.assertThat(s.matches(mapper.createArrayNode())).isFalse();
		Assertions.assertThat(s.matches(null)).isFalse();
	}
}
