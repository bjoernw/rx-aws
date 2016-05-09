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
package io.macgyver.reactor.aws.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.io.IOException;

public class RxJson {
    private final static Logger logger = LoggerFactory.getLogger(RxJson.class);
    private final static ObjectMapper mapper = new ObjectMapper();

    public static final Func1<String, Observable<JsonNode>> STRING_TO_JSON = t -> {
        try {
            return Observable.just(mapper.readTree(t));
        } catch (IOException e) {
            logger.info("could not parse: {}", e.toString());
        }
        return Observable.empty();
    };
}
