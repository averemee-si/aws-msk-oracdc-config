/**
 * Copyright (c) 2018-present, A2 Rešitve d.o.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package eu.solutions.a2.aws.msk.oracdc.config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import software.amazon.awssdk.utils.StringUtils;

public class PropertiesManager {

	private final Map<String, String> props;

	public PropertiesManager() {
		props = new LinkedHashMap<>();
		// Set default/hardcoded values
		props.put("offset.flush.interval.ms", "10000");
		props.put("offset.flush.timeout.ms", "5000");
		props.put("plugin.path", "/opt/kafka/connect/lib");
		props.put("internal.key.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("internal.value.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("internal.key.converter.schemas.enable", "false");
		props.put("internal.value.converter.schemas.enable", "false");		
		props.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
		props.put("key.converter.schemas.enable", "true");
		props.put("value.converter.schemas.enable", "true");		
	}

	public PropertiesManager(final String groupId,
			final String offsetTopic, final String configTopic, final String statusTopic,
			final String restPort, final String region, final String registryName) {
		this();
		setDistributed(groupId, offsetTopic, configTopic, statusTopic);
		props.put("rest.port", restPort);
		setGlueSchemaRegistry(region, registryName);
	}

	public void setBootstrapServers(final String bootstrapServers) {
		props.put("bootstrap.servers", bootstrapServers);
	}

	public void setGlueSchemaRegistry(
			final String region,
			final String registryName) {
		String schemaNamingClass = "eu.solutions.a2.aws.glue.schema.registry.KafkaConnectSchemaNamingStrategy";
		props.put("key.converter", "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter");
		props.put("key.converter.schemas.enable", "true");
		props.put("key.converter.schemaAutoRegistrationEnabled", "true");
		props.put("key.converter.avroRecordType", "GENERIC_RECORD");
		props.put("key.converter.region", region);
		props.put("key.converter.schemaNameGenerationClass", schemaNamingClass);
		if (!StringUtils.isBlank(registryName)) {
			props.put("key.converter.registry.name", registryName);
		}
		props.put("value.converter", "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter");
		props.put("value.converter.schemas.enable", "true");
		props.put("value.converter.schemaAutoRegistrationEnabled", "true");
		props.put("value.converter.avroRecordType", "GENERIC_RECORD");
		props.put("value.converter.region", region);
		props.put("value.converter.schemaNameGenerationClass", schemaNamingClass);
		if (!StringUtils.isBlank(registryName)) {
			props.put("value.converter.registry.name", registryName);
		}
	}

	public void setDistributed(
			final String groupId,
			final String offsetTopic,
			final String configTopic,
			final String statusTopic) {
		props.put("group.id", groupId);
		props.put("offset.storage.topic", offsetTopic);
		props.put("config.storage.topic", configTopic);
		props.put("status.storage.topic", statusTopic);
	}

	public void setAuthNone() {
		props.put("security.protocol", "PLAINTEXT");
		props.remove("sasl.mechanism");
		props.remove("sasl.jaas.config");
		props.remove("sasl.client.callback.handler.class");
		props.remove("ssl.truststore.location");
		props.remove("ssl.keystore.location");
		props.remove("ssl.keystore.password");
		props.remove("ssl.key.password");
		props.remove("producer.security.protocol");
		props.remove("producer.sasl.mechanism");
		props.remove("producer.sasl.jaas.config");
		props.remove("producer.sasl.client.callback.handler.class");
		props.remove("producer.ssl.truststore.location");
		props.remove("producer.ssl.keystore.location");
		props.remove("producer.ssl.keystore.password");
		props.remove("producer.ssl.key.password");
	}

	public void setAuthSsl() {
		props.put("security.protocol", "SSL");
		props.remove("sasl.mechanism");
		props.remove("sasl.jaas.config");
		props.remove("sasl.client.callback.handler.class");
		props.remove("ssl.truststore.location");
		props.remove("ssl.keystore.location");
		props.remove("ssl.keystore.password");
		props.remove("ssl.key.password");
		props.put("producer.security.protocol", "SSL");
		props.remove("producer.sasl.mechanism");
		props.remove("producer.sasl.jaas.config");
		props.remove("producer.sasl.client.callback.handler.class");
		props.remove("producer.ssl.truststore.location");
		props.remove("producer.ssl.keystore.location");
		props.remove("producer.ssl.keystore.password");
		props.remove("producer.ssl.key.password");
	}

	public void setAuthIam() {
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "AWS_MSK_IAM");
		props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
		props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
		props.remove("ssl.truststore.location");
		props.remove("ssl.keystore.location");
		props.remove("ssl.keystore.password");
		props.remove("ssl.key.password");
		props.put("producer.security.protocol", "SASL_SSL");
		props.put("producer.sasl.mechanism", "AWS_MSK_IAM");
		props.put("producer.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
		props.put("producer.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
		props.remove("producer.ssl.truststore.location");
		props.remove("producer.ssl.keystore.location");
		props.remove("producer.ssl.keystore.password");
		props.remove("producer.ssl.key.password");
	}

	public void setAuthScram(final String username, final String password) {
		String jaasConfig = String.format(
				"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
				username, password);
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "SCRAM-SHA-512");
		props.put("sasl.jaas.config", jaasConfig);
		props.remove("sasl.client.callback.handler.class");
		props.remove("ssl.truststore.location");
		props.remove("ssl.keystore.location");
		props.remove("ssl.keystore.password");
		props.remove("ssl.key.password");
		props.put("producer.security.protocol", "SASL_SSL");
		props.put("producer.sasl.mechanism", "SCRAM-SHA-512");
		props.put("producer.sasl.jaas.config", jaasConfig);
		props.remove("producer.sasl.client.callback.handler.class");
		props.remove("producer.ssl.truststore.location");
		props.remove("producer.ssl.keystore.location");
		props.remove("producer.ssl.keystore.password");
		props.remove("producer.ssl.key.password");
	}

	public void setAuthMutualTls(final String truststore, final String keystore,
			final String keystorePassword, final String keyPassword) {
		//TODO - check for file's on file system!
		props.put("security.protocol", "SSL");
		props.remove("sasl.mechanism");
		props.remove("sasl.jaas.config");
		props.remove("sasl.client.callback.handler.class");
		props.put("ssl.truststore.location", truststore);
		props.put("ssl.keystore.location", keystore);
		props.put("ssl.keystore.password", keystorePassword);
		props.put("ssl.key.password", keyPassword);
		props.put("producer.security.protocol", "SSL");
		props.remove("producer.sasl.mechanism");
		props.remove("producer.sasl.jaas.config");
		props.remove("producer.sasl.client.callback.handler.class");
		props.put("producer.ssl.truststore.location", truststore);
		props.put("producer.ssl.keystore.location", keystore);
		props.put("producer.ssl.keystore.password", keystorePassword);
		props.put("producer.ssl.key.password", keyPassword);
	}

	public void save(final String fileName) throws IOException {
		String comment = "# Generated by oracdc autoconfig " + Instant.now().toString();
		OutputStream fos = new FileOutputStream(fileName);
		PrintWriter pw=new PrintWriter(fos);
		pw.println(comment);
		props.forEach((k, v) -> pw.println(k + "=" + v));
		pw.flush();
		pw.close();
		fos.close();
	}

	@Override
	public String toString() {
		return props.toString();
	}
}
