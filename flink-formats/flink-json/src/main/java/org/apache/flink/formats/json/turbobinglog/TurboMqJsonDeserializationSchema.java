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

package org.apache.flink.formats.json.turbobinglog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.turbobinglog.TurboMqJsonDecodingFormat.ReadableMetadata;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.flink.formats.json.turbobinglog.TurboMqJsonDecodingFormat.BINGLOGFORMATPREFIX;

/**
 * Deserialization schema from Debezium JSON to Flink Table/SQL internal data structure {@link
 * RowData}. The deserialization schema knows Debezium's schema definition and can extract the
 * database data and convert into {@link RowData} with {@link RowKind}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
@Internal
public final class TurboMqJsonDeserializationSchema implements DeserializationSchema<RowData> {
    private static final long serialVersionUID = 1L;


    /**
     * The deserializer to deserialize Debezium JSON data.
     */
    private final JsonRowDataDeserializationSchema jsonDeserializer;

    /**
     * Flag that indicates that an additional projection is required for metadata.
     */
    private final boolean hasMetadata;

    /**
     * Metadata to be extracted for every record.
     */
    private final MetadataConverter[] metadataConverters;

    /**
     * {@link TypeInformation} of the produced {@link RowData} (physical + meta data).
     */
    private final TypeInformation<RowData> producedTypeInfo;

    /**
     * Flag indicating whether the Debezium JSON data contains schema part or not. When Debezium
     * Kafka Connect enables "value.converter.schemas.enable", the JSON will contain "schema"
     * information, but we just ignore "schema" and extract data from "payload".
     */
    /**
     * Flag indicating whether to ignore invalid fields/rows (default: throw an exception).
     */
    private final boolean ignoreParseErrors;

    private final Map<Integer, Integer> realFieldPositionMap;
    private final Map<Integer, Integer> metaPositionMap;

    public TurboMqJsonDeserializationSchema(
            DataType dataType,
            DataType physicalDataType,
            List<ReadableMetadata> readableMetadata,
            TypeInformation<RowData> producedTypeInfo,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {

        this.realFieldPositionMap = new HashMap<>();
        this.metaPositionMap = new HashMap<>();
        this.realDataPosition(dataType);
        // rowType for mq json that contains rowData and base
        final RowType jsonRowType =
                createJsonRowType(physicalDataType, readableMetadata);
        this.jsonDeserializer =
                new JsonRowDataDeserializationSchema(
                        jsonRowType,
                        // the result type is never used, so it's fine to pass in the produced type
                        // info
                        producedTypeInfo,
                        false, // ignoreParseErrors already contains the functionality of
                        // failOnMissingField
                        ignoreParseErrors,
                        true,
                        timestampFormat);
        this.hasMetadata = readableMetadata.size() > 0;
        this.metadataConverters =
                createMetadataConverters(jsonRowType, readableMetadata);
        this.producedTypeInfo = producedTypeInfo;
        this.ignoreParseErrors = ignoreParseErrors;
    }

    private static RowType createJsonRowType(
            DataType physicalDataType, List<ReadableMetadata> readableMetadata) {
        ResolvedSchema tableSchema = DataTypeUtils.expandCompositeTypeToSchema(physicalDataType);
        List<DataTypes.Field> fields = new ArrayList<>();
        tableSchema.getColumns().forEach(c -> {
            fields.add(DataTypes.FIELD(c.getName(), DataTypes.ROW(DataTypes.FIELD("v", c.getDataType()))));
        });
        DataType innerRow = DataTypes.ROW(fields.toArray(new DataTypes.Field[fields.size()]));
        DataType payload =
                DataTypes.ROW(
                        DataTypes.FIELD("rowData", DataTypes.ARRAY(innerRow))
                );

        // append fields that are required for reading metadata in the payload
        final List<DataTypes.Field> payloadMetadataFields =
                readableMetadata.stream()
                        .filter(m -> m.isJsonPayload)
                        .map(m -> m.requiredJsonField)
                        .distinct()
                        .collect(Collectors.toList());
        payload = DataTypeUtils.appendRowFields(payload, payloadMetadataFields);
        return (RowType) payload.getLogicalType();
    }

    private static MetadataConverter[] createMetadataConverters(
            RowType jsonRowType, List<ReadableMetadata> requestedMetadata) {
        return requestedMetadata.stream()
                .map(m -> convertInPayload(jsonRowType, m))
                .toArray(MetadataConverter[]::new);
    }

    private static MetadataConverter convertInRoot(RowType jsonRowType, ReadableMetadata metadata) {
        final int pos = findFieldPos(metadata, jsonRowType);
        return new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            @Override
            public Object convert(GenericRowData root, int unused) {
                try {
                    return metadata.converter.convert(root, pos);
                } catch (Exception e) {
                    throw new RuntimeException("current meta:" + metadata.key, e);
                }

            }
        };
    }

    private static MetadataConverter convertInPayload(
            RowType jsonRowType, ReadableMetadata metadata) {
        return convertInRoot(jsonRowType, metadata);
    }

    private static int findFieldPos(ReadableMetadata metadata, RowType jsonRowType) {
        return jsonRowType.getFieldNames().indexOf(metadata.requiredJsonField.getName());
    }

    private void realDataPosition(DataType dataType) {
        final RowType rowType = (RowType) dataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        int realTypePosition = 0;
        int metaPosition = 0;
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().startsWith(BINGLOGFORMATPREFIX)) {
                metaPositionMap.put(metaPosition, i);
                metaPosition++;
            } else {
                realFieldPositionMap.put(realTypePosition, i);
                realTypePosition++;
            }
        }
    }

    @Override
    public RowData deserialize(byte[] message) {
        throw new RuntimeException(
                "Please invoke DeserializationSchema#deserialize(byte[], Collector<RowData>) instead.");
    }

    @Override
    public void deserialize(byte[] message, Collector<RowData> out) throws IOException {
        if (message == null || message.length == 0) {
            // skip tombstone messages
            return;
        }
        if (new String(message, StandardCharsets.UTF_8).endsWith("_full_eof")) {
            return;
        }
        try {
            GenericRowData payload = (GenericRowData) jsonDeserializer.deserialize(message);
            // "rowData"
            ArrayData arrayData = payload.getArray(0);
            for (int i = 0; i < arrayData.size(); i++) {
                GenericRowData innerRow = (GenericRowData) arrayData.getRow(i, 0);
                final GenericRowData producedRow =
                        new GenericRowData(payload.getRowKind(), innerRow.getArity());
                for (int j = 0; j < innerRow.getArity(); j++) {
                    GenericRowData fieldRow = (GenericRowData) innerRow.getRow(j, 1);
                    if (Objects.isNull(fieldRow)) {
                        producedRow.setField(j, null);
//                        throw new IOException("check field name json is Case sensitivity !!,rowIndex:" + j + ",row:" + innerRow.toString());
                    } else {
                        // value
                        producedRow.setField(j, fieldRow.getField(0));
                    }
                }
                // field-name
                emitRow(payload, producedRow, out);
            }
        } catch (Throwable t) {
            // a big try catch to protect the processing.
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Corrupt TurboMq binglog JSON message '%s'.", new String(message)), t);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private void emitRow(
            GenericRowData rootRow, GenericRowData physicalRow, Collector<RowData> out) {
        // shortcut in case no output projection is required
        if (!hasMetadata) {
            out.collect(physicalRow);
            return;
        }

        final int physicalArity = physicalRow.getArity();
        final int metadataArity = metadataConverters.length;

        final GenericRowData producedRow =
                new GenericRowData(physicalRow.getRowKind(), physicalArity + metadataArity);

        for (int physicalPos = 0; physicalPos < physicalArity; physicalPos++) {
            producedRow.setField(realFieldPositionMap.get(physicalPos), physicalRow.getField(physicalPos));
        }

        for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
            producedRow.setField(
                    metaPositionMap.get(metadataPos), metadataConverters[metadataPos].convert(rootRow));
        }

        out.collect(producedRow);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TurboMqJsonDeserializationSchema that = (TurboMqJsonDeserializationSchema) o;
        return Objects.equals(jsonDeserializer, that.jsonDeserializer)
                && hasMetadata == that.hasMetadata
                && Objects.equals(producedTypeInfo, that.producedTypeInfo)
                && ignoreParseErrors == that.ignoreParseErrors;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jsonDeserializer, hasMetadata, producedTypeInfo, ignoreParseErrors);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Converter that extracts a metadata field from the row (root or payload) that comes out of the
     * JSON schema and converts it to the desired data type.
     */
    interface MetadataConverter extends Serializable {

        // Method for top-level access.
        default Object convert(GenericRowData row) {
            return convert(row, -1);
        }

        Object convert(GenericRowData row, int pos);
    }
}
