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

package io.pravega.connectors.presto.util;

import io.pravega.connectors.presto.ObjectType;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaTableHandle;
import io.pravega.shared.NameUtils;

import java.util.HashMap;
import java.util.Map;

public class PravegaNameUtils
{
    private PravegaNameUtils()
    {
    }

    public static final String STREAM_CUT_PREFIX = "-SC";

    // used for prefixing field names when presenting them in presto
    // will default to these prefixes for kv table fields unless specified in user config
    static Map<Integer, String> kvFieldNamePrefixMap = new HashMap<>();
    static
    {
        kvFieldNamePrefixMap.put(0, "key");
        kvFieldNamePrefixMap.put(1, "value");
    }

    public static String scopedName(String scope, String stream)
    {
        return scope + "/" + stream;
    }

    public static String groupId(String scope, String stream)
    {
        return scope + "." + stream;
    }

    // test stream name - if not valid pravega stream name assume it is regex for multi source
    public static boolean multiSourceStream(PravegaStreamDescription object)
    {
        return object.getObjectType() == ObjectType.STREAM &&
                multiSourceStream(object.getObjectName());
    }

    public static boolean multiSourceStream(PravegaTableHandle object)
    {
        return object.getObjectType() == ObjectType.STREAM &&
                multiSourceStream(object.getObjectName());
    }

    private static boolean multiSourceStream(String stream)
    {
        try {
            // test pattern for stream names pravega will allow
            NameUtils.validateUserStreamName(stream);
            return false;
        }
        catch (IllegalArgumentException e) {
            // if not valid, we take it as multi source w/ regex
            return true;
        }
    }

    public static boolean kvTable(PravegaStreamDescription object)
    {
        return object.getObjectType() == ObjectType.KV_TABLE;
    }

    public static String kvFieldMapping(int index)
    {
        // should only see 0, 1
        return kvFieldNamePrefixMap.getOrDefault(index, "UNEXPECTED INDEX");
    }

    // need to allow user/application to specify what table name is
    // https://github.com/StreamingDataPlatform/pravega-sql/issues/91
    public static String temp_tableNameToStreamName(String tableName)
    {
        // stream name does not allow '_' so we change to '-'
        // but tpc test + scripts require '_'
        return tableName.replaceAll("_", "-");
    }

    public static String temp_streamNameToTableName(String streamName)
    {
        // stream name does not allow '_' so we change to '-'
        // but tpc test + scripts require '_'
        return streamName.replaceAll("-", "_");
    }

    public static String streamCutName(String stream)
    {
        return stream + STREAM_CUT_PREFIX;
    }
}
