/*
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
package com.facebook.presto.pravega.util;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.PrestoException;
import com.google.common.io.CharStreams;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Locale.ENGLISH;

public class PravegaSchemaUtils
{
    private PravegaSchemaUtils()
    {
    }

    private static final Logger log = Logger.get(PravegaSchemaUtils.class);

    public static final String AVRO_INLINE = "avro-inline";
    public static final String PROTOBUF_INLINE = "protobuf-inline";
    public static final String JSON_INLINE = "json-inline";
    public static final String INLINE_SUFFIX = "-inline";
    public static final String GROUP_PROPERTIES_INLINE_KEY = "inline";
    public static final String GROUP_PROPERTIES_INLINE_KV_KEY = "inlinekey";
    public static final String GROUP_PROPERTIES_INLINE_KV_VALUE = "inlinevalue";
    public static final String AVRO = "avro";
    public static final String PROTOBUF = "protobuf";
    public static final String JSON = "json";
    public static final String CSV = "csv";

    public static final String NESTED_RECORD_SEPARATOR = "/";

    public static String readSchema(String dataSchemaLocation)
    {
        InputStream inputStream = null;
        try {
            if (isURI(dataSchemaLocation.trim().toLowerCase(ENGLISH))) {
                try {
                    inputStream = new URL(dataSchemaLocation).openStream();
                }
                catch (MalformedURLException e) {
                    // try again before failing
                    log.warn("invalid URL: " + dataSchemaLocation);
                    inputStream = new FileInputStream(dataSchemaLocation);
                }
            }
            else {
                inputStream = new FileInputStream(dataSchemaLocation);
            }
            return CharStreams.toString(new InputStreamReader(inputStream, UTF_8));
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    "Could not parse the schema at: " + dataSchemaLocation, e);
        }
        finally {
            closeQuietly(inputStream);
        }
    }

    private static void closeQuietly(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isURI(String location)
    {
        try {
            URI.create(location);
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }
}
