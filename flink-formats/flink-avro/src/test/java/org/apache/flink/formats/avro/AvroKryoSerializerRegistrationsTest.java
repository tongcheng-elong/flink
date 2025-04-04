/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests that the set of Kryo registrations is the same across compatible Flink versions.
 *
 * <p>Special version of {@code KryoSerializerRegistrationsTest} that sits in the Avro module and
 * verifies that we correctly register Avro types at the {@link KryoSerializer} when Avro is
 * present.
 */
class AvroKryoSerializerRegistrationsTest {

    /**
     * Tests that the registered classes in Kryo did not change.
     *
     * <p>Once we have proper serializer versioning this test will become obsolete. But currently a
     * change in the serializers can break savepoint backwards compatibility between Flink versions.
     */
    @Test
    void testDefaultKryoRegisteredClassesDidNotChange() throws Exception {
        final Kryo kryo = new KryoSerializer<>(Integer.class, new SerializerConfigImpl()).getKryo();

        try (BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                getClass()
                                        .getClassLoader()
                                        .getResourceAsStream("flink_11-kryo_registrations")))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(",");
                final int tag = Integer.parseInt(split[0]);
                final String registeredClass = split[1];

                Registration registration = kryo.getRegistration(tag);

                if (registration == null) {
                    fail(String.format("Registration for %d = %s got lost", tag, registeredClass));
                } else if (!registeredClass.equals(registration.getType().getName())) {
                    fail(
                            String.format(
                                    "Registration for %d = %s changed to %s",
                                    tag, registeredClass, registration.getType().getName()));
                }
            }
        }
    }

    @Test
    void testEnableForceKryoAvroRegister() {
        ExecutionConfig executionConfig = new ExecutionConfig();
        ((SerializerConfigImpl) executionConfig.getSerializerConfig()).setForceKryoAvro(true);
        final Kryo kryo =
                new KryoSerializer<>(Integer.class, executionConfig.getSerializerConfig())
                        .getKryo();
        kryo.setRegistrationRequired(true);
        assertThatCode(() -> kryo.getRegistration(GenericData.Array.class))
                .doesNotThrowAnyException();
    }

    @Test
    void testDefaultForceKryoAvroRegister() {
        ExecutionConfig executionConfig = new ExecutionConfig();
        final Kryo kryo =
                new KryoSerializer<>(Integer.class, executionConfig.getSerializerConfig())
                        .getKryo();
        kryo.setRegistrationRequired(true);
        assertThatCode(() -> kryo.getRegistration(GenericData.Array.class))
                .doesNotThrowAnyException();
    }

    @Test
    void testDisableForceKryoAvroRegister() {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.FORCE_KRYO_AVRO, false);
        ExecutionConfig executionConfig = new ExecutionConfig(configuration);
        final Kryo kryo =
                new KryoSerializer<>(Integer.class, executionConfig.getSerializerConfig())
                        .getKryo();
        kryo.setRegistrationRequired(true);
        assertThatThrownBy(() -> kryo.getRegistration(GenericData.Array.class))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Creates a Kryo serializer and writes the default registrations out to a comma separated file
     * with one entry per line:
     *
     * <pre>
     * id,class
     * </pre>
     *
     * <p>The produced file is used to check that the registered IDs don't change in future Flink
     * versions.
     *
     * <p>This method is not used in the tests, but documents how the test file has been created and
     * can be used to re-create it if needed.
     *
     * @param filePath File path to write registrations to
     */
    private void writeDefaultKryoRegistrations(String filePath) throws IOException {
        final File file = new File(filePath);
        if (file.exists()) {
            assertThat(file.delete()).isTrue();
        }

        final Kryo kryo = new KryoSerializer<>(Integer.class, new SerializerConfigImpl()).getKryo();
        final int nextId = kryo.getNextRegistrationId();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (int i = 0; i < nextId; i++) {
                Registration registration = kryo.getRegistration(i);
                String str = registration.getId() + "," + registration.getType().getName();
                writer.write(str, 0, str.length());
                writer.newLine();
            }

            System.out.println("Created file with registrations at " + file.getAbsolutePath());
        }
    }
}
