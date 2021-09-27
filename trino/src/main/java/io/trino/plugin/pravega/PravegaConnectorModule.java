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

package io.trino.plugin.pravega;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.decoder.DecoderModule;
import io.trino.plugin.pravega.decoder.JsonRowDecoderFactory;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.HashSet;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;
import static java.util.Objects.requireNonNull;

/**
 * Guice module for the Pravega connector.
 */
public class PravegaConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(PravegaMetadata.class).in(Scopes.SINGLETON);
        binder.bind(PravegaSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(PravegaRecordSetProvider.class).in(Scopes.SINGLETON);

        binder.bind(PravegaSegmentManager.class).in(Scopes.SINGLETON);
        binder.bind(JsonRowDecoderFactory.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PravegaConnectorConfig.class);

        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
        jsonCodecBinder(binder).bindJsonCodec(PravegaStreamDescription.class);

        binder.install(new DecoderModule());
    }

    public static final class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        private final TypeManager typeManager;

        @Inject
        public TypeDeserializer(TypeManager typeManager)
        {
            super(Type.class);
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            return typeManager.getType(parseTypeSignature(value, new HashSet<>()));
        }
    }
}
