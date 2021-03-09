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

import com.google.common.base.Strings;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import io.pravega.schemaregistry.common.NameUtil;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;

public class ProtobufCommon
{
    private ProtobufCommon()
    {
    }

    public static String encodeSchema(final SchemaWithVersion schemaWithVersion)
    {
        return schemaWithVersion.getSchemaInfo().getType() + "|" +
                Base64.getEncoder().encodeToString(schemaWithVersion.getSchemaInfo().getSchemaData().array());
    }

    public static Pair<String, ByteBuffer> decodeSchema(String encodedSchema)
    {
        String[] parts = encodedSchema.split("\\|");
        return new ImmutablePair<>(parts[0], ByteBuffer.wrap(Base64.getDecoder().decode((parts[1]))));
    }

    public static Descriptors.Descriptor descriptorFor(final SchemaWithVersion schemaWithVersion)
    {
        return descriptorFor(schemaWithVersion.getSchemaInfo().getType(),
                schemaWithVersion.getSchemaInfo().getSchemaData());
    }

    // TODO: following code block is from schema registry/serializers.  license added above
    //       looking into possibly making this common/exposed in SR libs.
    public static Descriptors.Descriptor descriptorFor(String type, final ByteBuffer buffer)
    {
        DescriptorProtos.FileDescriptorSet descriptorSet;
        try {
            descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(buffer);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        int count = descriptorSet.getFileCount();
        String[] tokens = NameUtil.extractNameAndQualifier(type);
        String name = tokens[0];
        String pckg = tokens[1];
        DescriptorProtos.FileDescriptorProto mainDescriptor = null;
        for (DescriptorProtos.FileDescriptorProto x : descriptorSet.getFileList()) {
            boolean packageMatch;
            if (x.getPackage() == null) {
                packageMatch = Strings.isNullOrEmpty(pckg);
            }
            else {
                packageMatch = x.getPackage().equals(pckg);
            }
            if (packageMatch && x.getMessageTypeList().stream().anyMatch(y -> y.getName().equals(name))) {
                mainDescriptor = x;
                break;
            }
        }
        if (mainDescriptor == null) {
            throw new IllegalArgumentException("FileDescriptorSet doesn't contain the schema for the object type.");
        }

        Descriptors.FileDescriptor[] dependencyArray = new Descriptors.FileDescriptor[count];
        Descriptors.FileDescriptor fd;
        try {
            for (int i = 0; i < count; i++) {
                fd = Descriptors.FileDescriptor.buildFrom(
                        descriptorSet.getFile(i),
                        new Descriptors.FileDescriptor[]{});
                dependencyArray[i] = fd;
            }
            fd = Descriptors.FileDescriptor.buildFrom(mainDescriptor, dependencyArray);
        }
        catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Invalid protobuf schema.");
        }

        return fd.getMessageTypes().stream()
                .filter(x -> x.getName().equals(name)).findAny()
                .orElseThrow(() ->
                        new SerializationException(String.format("schema for %s not found", type)));
    }
}
