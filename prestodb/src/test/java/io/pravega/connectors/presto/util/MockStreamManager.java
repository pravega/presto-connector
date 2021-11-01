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

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.DeleteScopeFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * this class has a very limited use in unit testing, so many methods not implemented
 */
public class MockStreamManager
        implements StreamManager {

    private final List<String> scopes = new ArrayList<>();

    private final List<Stream> streams = new ArrayList<>();

    @Override
    public boolean createStream(String s, String s1, StreamConfiguration streamConfiguration)
    {
        Stream stream = Stream.of(s, s1);
        return !streams.contains(stream) && streams.add(stream);
    }

    @Override
    public boolean updateStream(String s, String s1, StreamConfiguration streamConfiguration)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean truncateStream(String s, String s1, StreamCut streamCut)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean sealStream(String s, String s1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteStream(String s, String s1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<String> listScopes()
    {
        return scopes.iterator();
    }

    @Override
    public boolean createScope(String s)
    {
        return !scopes.contains(s) && scopes.add(s);
    }

    @Override
    public boolean checkScopeExists(String s)
    {
        return scopes.contains(s);
    }

    @Override
    public Iterator<Stream> listStreams(String s)
    {
        return streams.stream().filter(stream -> stream.getScope().equals(s)).iterator();
    }

    @Override
    public boolean checkStreamExists(String s, String s1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteScope(String s)
    {
        return scopes.remove(s);
    }

    @Override
    public boolean deleteScope(String s, boolean b) throws DeleteScopeFailedException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamInfo getStreamInfo(String s, String s1)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {

    }
}
