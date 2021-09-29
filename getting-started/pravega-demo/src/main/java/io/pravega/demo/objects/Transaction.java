/*
 * Copyright (c) Pravega Authors.
 *
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
package io.pravega.demo.objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Transaction {
    private final int txnId;
    private final int productId;
    private final String location;
    private final long timestamp;

    @JsonCreator
    public Transaction(@JsonProperty("txnId") int txnId,
                       @JsonProperty("productId") int productId,
                       @JsonProperty("location") String location,
                       @JsonProperty("timestamp") long timestamp) {
        this.txnId = txnId;
        this.productId = productId;
        this.location = location;
        this.timestamp = timestamp;
    }

    @JsonProperty
    public int getTxnId() {
        return txnId;
    }

    @JsonProperty
    public int getProductId() {
        return productId;
    }

    @JsonProperty
    public String getLocation() {
        return location;
    }

    @JsonProperty
    public long getTimestamp() {
        return timestamp;
    }
}