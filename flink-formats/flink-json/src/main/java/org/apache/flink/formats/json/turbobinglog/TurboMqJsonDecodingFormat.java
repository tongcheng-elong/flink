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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.turbobinglog.TurboMqJsonDeserializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link DecodingFormat} for Debezium using JSON encoding. */
public class TurboMqJsonDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    public static final String BINGLOGFORMATPREFIX = "binlog_";
    // --------------------------------------------------------------------------------------------
    // Mutable attributes
    // --------------------------------------------------------------------------------------------
    private static final StringData KEY_SOURCE_TIMESTAMP = StringData.fromString("ts_ms");

    // --------------------------------------------------------------------------------------------
    // Debezium-specific attributes
    // --------------------------------------------------------------------------------------------
    private static final StringData KEY_SOURCE_DATABASE = StringData.fromString("db");
    private static final StringData KEY_SOURCE_SCHEMA = StringData.fromString("schema");
    private static final StringData KEY_SOURCE_TABLE = StringData.fromString("table");
    private final boolean ignoreParseErrors;
    private final TimestampFormat timestampFormat;
    private List<String> metadataKeys;

    public TurboMqJsonDecodingFormat(boolean ignoreParseErrors, TimestampFormat timestampFormat) {
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.metadataKeys = Collections.emptyList();
    }

    private static DataType extraRealDataType(DataType dataType) {
        final RowType rowType = (RowType) dataType.getLogicalType();
        return TypeConversions.fromLogicalToDataType(
                new RowType(
                        false,
                        rowType.getFields().stream()
                                .filter(s -> !s.getName().startsWith(BINGLOGFORMATPREFIX))
                                .collect(Collectors.toList())));
    }

    private static List<ReadableMetadata> extraDataTypeMeta(DataType dataType) {
        final RowType rowType = (RowType) dataType.getLogicalType();
        return rowType.getFields().stream()
                .filter(s -> s.getName().startsWith(BINGLOGFORMATPREFIX))
                .map(s -> s.getName())
                .map(
                        k ->
                                Stream.of(ReadableMetadata.values())
                                        .filter(rm -> rm.key.equals(k))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .collect(Collectors.toList());
    }

    // --------------------------------------------------------------------------------------------
    // Metadata handling
    // --------------------------------------------------------------------------------------------

    private static Object readProperty(GenericRowData row, int pos, StringData key) {
        final GenericMapData map = (GenericMapData) row.getMap(pos);
        if (map == null) {
            return null;
        }
        return map.get(key);
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context, DataType physicalDataType) {
        final List<ReadableMetadata> readableMetadata = extraDataTypeMeta(physicalDataType);
        final DataType realDataType = extraRealDataType(physicalDataType);

        final TypeInformation<RowData> producedTypeInfo =
                context.createTypeInformation(physicalDataType);

        return new TurboMqJsonDeserializationSchema(
                physicalDataType,
                realDataType,
                readableMetadata,
                producedTypeInfo,
                ignoreParseErrors,
                timestampFormat);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        final Map<String, DataType> metadataMap = new LinkedHashMap<>();
        Stream.of(ReadableMetadata.values())
                .forEachOrdered(m -> metadataMap.put(m.key, m.dataType));
        return metadataMap;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys) {
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                //                .addContainedKind(RowKind.UPDATE_BEFORE)
                //                .addContainedKind(RowKind.UPDATE_AFTER)
                //                .addContainedKind(RowKind.DELETE)
                .build();
    }

    /** List of metadata that can be read with this format. */
    enum ReadableMetadata {
        SCHEMA(
                "binlog_schema",
                DataTypes.STRING().nullable(),
                true,
                DataTypes.FIELD("schema", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),

        TABLE_NAME(
                "binlog_tablename",
                DataTypes.STRING().nullable(),
                true,
                DataTypes.FIELD("tableName", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),

        INDICATOR(
                "binlog_indicator",
                DataTypes.BIGINT(),
                true,
                DataTypes.FIELD("indicator", DataTypes.BIGINT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        if (row.isNullAt(pos)) {
                            return null;
                        }
                        return row.getLong(pos);
                    }
                }),

        EXECUTE_TIME(
                "binlog_executetime",
                DataTypes.BIGINT(),
                true,
                DataTypes.FIELD("executeTime", DataTypes.BIGINT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getLong(pos);
                    }
                }),
        SUBSCRIBE_TYPE(
                "binlog_subscribetype",
                DataTypes.BIGINT(),
                true,
                DataTypes.FIELD("subscribeType", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),

        EVENT_TYPE(
                "binlog_eventtype",
                DataTypes.STRING().nullable(),
                true,
                DataTypes.FIELD("eventType", DataTypes.STRING()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getString(pos);
                    }
                }),
        INBOUND_INDEX(
                "binlog_inboundindex",
                DataTypes.INT().nullable(),
                true,
                DataTypes.FIELD("inboundIndex", DataTypes.INT()),
                new MetadataConverter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Object convert(GenericRowData row, int pos) {
                        return row.getInt(pos);
                    }
                });

        final String key;

        final DataType dataType;

        final boolean isJsonPayload;

        final DataTypes.Field requiredJsonField;

        final MetadataConverter converter;

        ReadableMetadata(
                String key,
                DataType dataType,
                boolean isJsonPayload,
                DataTypes.Field requiredJsonField,
                MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.isJsonPayload = isJsonPayload;
            this.requiredJsonField = requiredJsonField;
            this.converter = converter;
        }
    }
}
