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

package com.facebook.presto.pravega.decoder;

import com.facebook.presto.pravega.DelimRecordValue;
import com.facebook.presto.pravega.PravegaRecordValue;

public class CsvRowDecoder
        implements EventDecoder
{
    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        ((DelimRecordValue) record).setBuf(event.asBytes());
        return true;
    }
}
