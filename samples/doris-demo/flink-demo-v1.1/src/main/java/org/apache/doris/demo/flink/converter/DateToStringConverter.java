// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.doris.demo.flink.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.function.Consumer;

public class DateToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
  private static final Logger log = LoggerFactory.getLogger(DateToStringConverter.class);
  private DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_DATE;
  private DateTimeFormatter timeFormatter = DateTimeFormatter.ISO_TIME;
  private DateTimeFormatter datetimeFormatter = DateTimeFormatter.ISO_DATE_TIME;
  private DateTimeFormatter timestampFormatter = DateTimeFormatter.ISO_DATE_TIME;
  private ZoneId timestampZoneId = ZoneId.systemDefault();

  public static Properties DEFAULT_PROPS = new Properties();

  static {
    DEFAULT_PROPS.setProperty("converters", "date");
    DEFAULT_PROPS.setProperty("date.type", "org.apache.doris.demo.flink.converter.DateToStringConverter");
    DEFAULT_PROPS.setProperty("date.format.date", "yyyy-MM-dd");
    DEFAULT_PROPS.setProperty("date.format.datetime", "yyyy-MM-dd HH:mm:ss");
    DEFAULT_PROPS.setProperty("date.format.timestamp", "yyyy-MM-dd HH:mm:ss");
    DEFAULT_PROPS.setProperty("date.format.timestamp.zone", "UTC");
  }

  @Override
  public void configure(Properties props) {
    readProps(props, "format.date", p -> dateFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.time", p -> timeFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.datetime", p -> datetimeFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.timestamp", p -> timestampFormatter = DateTimeFormatter.ofPattern(p));
    readProps(props, "format.timestamp.zone", z -> timestampZoneId = ZoneId.of(z));
  }

  private void readProps(Properties properties, String settingKey, Consumer<String> callback) {
    String settingValue = (String) properties.get(settingKey);
    if (settingValue == null || settingValue.length() == 0) {
      return;
    }
    try {
      callback.accept(settingValue.trim());
    } catch (IllegalArgumentException | DateTimeException e) {
      log.error("setting {} is illegal:{}", settingKey, settingValue);
      throw e;
    }
  }

  @Override
  public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
    String sqlType = column.typeName().toUpperCase();
    SchemaBuilder schemaBuilder = null;
    Converter converter = null;
    if ("DATE".equals(sqlType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertDate;
    }
    if ("TIME".equals(sqlType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertTime;
    }
    if ("DATETIME".equals(sqlType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertDateTime;
    }
    if ("TIMESTAMP".equals(sqlType)) {
      schemaBuilder = SchemaBuilder.string().optional();
      converter = this::convertTimestamp;
    }
    if (schemaBuilder != null) {
      registration.register(schemaBuilder, converter);
    }
  }

  private String convertDate(Object input) {
    if (input instanceof LocalDate) {
      return dateFormatter.format((LocalDate) input);
    } else if (input instanceof Integer) {
      LocalDate date = LocalDate.ofEpochDay((Integer) input);
      return dateFormatter.format(date);
    }
    return null;
  }

  private String convertTime(Object input) {
    if (input instanceof Duration) {
      Duration duration = (Duration) input;
      long seconds = duration.getSeconds();
      int nano = duration.getNano();
      LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
      return timeFormatter.format(time);
    }
    return null;
  }

  private String convertDateTime(Object input) {
    if (input instanceof LocalDateTime) {
      return datetimeFormatter.format((LocalDateTime) input);
    } else if (input instanceof Timestamp) {
      return datetimeFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    return null;
  }

  private String convertTimestamp(Object input) {
    if (input instanceof ZonedDateTime) {
      // mysql timestamp will be converted to UTC storage,and the zonedDatetime here is UTC time
      ZonedDateTime zonedDateTime = (ZonedDateTime) input;
      LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
      return timestampFormatter.format(localDateTime);
    } else if (input instanceof Timestamp) {
      return timestampFormatter.format(((Timestamp) input).toLocalDateTime());
    }
    return null;
  }

}
