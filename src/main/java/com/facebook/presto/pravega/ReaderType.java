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

package com.facebook.presto.pravega;

public enum ReaderType
{
    SEGMENT_RANGE             /* 1 split handles all segments within a stream cut */,
    SEGMENT_RANGE_PER_SPLIT   /* segments for stream cut are given out to different splits */,
    EVENT_STREAM              /* stream oriented reading (vs. segments) */,
    SINGLE_GROUP_EVENT_STREAM /* stream oriented reading (vs. segments) all readers in same group */,
    KVT                       /* key value table */,
}
