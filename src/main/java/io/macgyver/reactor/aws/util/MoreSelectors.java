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

import com.fasterxml.jackson.databind.JsonNode;

import reactor.bus.selector.HeaderResolver;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.fn.Consumer;
import reactor.fn.Predicate;

public class MoreSelectors {

	public static Selector<JsonNode> jsonPredicate(Predicate<JsonNode> n) {
		return typedPredicate(n);
	}
	
	/**
	 * Generic version of Selectors.predicate()
	 * @param p
	 * @return
	 */
	public static <T> Selector<T> typedPredicate(final Predicate<T> p) {
		Predicate<Object> x = new Predicate<Object>() {

			@Override
			public boolean test(Object t) {
				if (t != null) {
					try {
						return p.test((T) t);
					} catch (ClassCastException e) {
						// Not really a problem. Due to Java's type erasure,
						// can't do an instnaceof check, so this will
						// have to do.
					}
				}
				return false;
			}

		};

		return Selectors.predicate(x);
	}

	public static Selector all(Selector ...selectors) {
		Selector composite = new Selector() {

			@Override
			public Object getObject() {
	
				return this;
			}

			@Override
			public boolean matches(Object key) {
			
				for (Selector s: selectors) {
					if (!s.matches(key)) {
						return false;
					}
				}
				return true;
				
			}

			@Override
			public HeaderResolver getHeaderResolver() {
				// TODO Auto-generated method stub
				return null;
			}
			
		};
		
		return composite;
	}
}
