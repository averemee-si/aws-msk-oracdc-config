package eu.solutions.a2.aws.msk.oracdc.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.lanterna.gui2.ComboBox;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.RadioBoxList;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kafka.model.ClientAuthentication;
import software.amazon.awssdk.services.kafka.model.ClusterInfo;
import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.services.kafka.model.ListClustersResponse;
import software.amazon.awssdk.services.kafka.model.ListNodesRequest;
import software.amazon.awssdk.services.kafka.model.ListNodesResponse;
import software.amazon.awssdk.services.kafka.model.NodeInfo;
import software.amazon.awssdk.services.kafka.model.NodeType;
import software.amazon.awssdk.utils.StringUtils;

public class AwsObjects {

	private static final Logger LOGGER = LoggerFactory.getLogger(AwsObjects.class);
	private static final String MSK_CONSOLE = "https://%s.console.aws.amazon.com/msk/home?region=%s#/cluster/create";

	protected static final String PRIVATE_ACCESS = "PRIVATE";
	protected static final String PUBLIC_ACCESS = "PUBLIC";

	private final String regionName;
	final KafkaClient kafkaClient;
	private ListClustersResponse listClusters = null;

	public static String getRegion() throws IOException {
		final URL url = new URL("http://169.254.169.254/latest/meta-data/placement/availability-zone/");
		final HttpURLConnection http = (HttpURLConnection)url.openConnection();
		// Instance metadata is here ;-)
		if (http.getResponseCode() == 200) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(http.getInputStream()));
			final String az = reader.readLine();
			LOGGER.debug("Availability zone = {}", az);
			return az.substring(0, az.length() - 1);
		} else {
			throw new IOException(http.getResponseMessage());
		}
	}

	public AwsObjects(final String regionName) {
		this.regionName = regionName;
		this.kafkaClient = KafkaClient.builder()
				.region(Region.of(regionName))
				.build();
		try {
			listClusters = kafkaClient.listClusters();
		} catch (KafkaException ke) {
			LOGGER.error(ke.getMessage());
			final StringWriter sw = new StringWriter();
			final PrintWriter pw = new PrintWriter(sw);
			ke.printStackTrace(pw);
			LOGGER.error(sw.toString());
		}
	}

	public void getClustersList(final ComboBox<String> clusters) {
		if (listClusters == null || listClusters.clusterInfoList().isEmpty()) {
			LOGGER.error("No MSK clustersfound!");
			LOGGER.error("Please visit {} and create MSK cluster!",
					String.format(MSK_CONSOLE, regionName, regionName));
		} else {
			clusters.clearItems();
			listClusters.clusterInfoList().forEach(ci -> 
				clusters.addItem(ci.clusterName()));
		}
	}

	public void setDependent(
			final int selectedIndex,
			final Label clusterArn,
			final RadioBoxList<String> clusterAccess,
			final ComboBox<String> clusterAuth) {
		// ARN
		ClusterInfo cluster = listClusters.clusterInfoList().get(selectedIndex);
		clusterArn.setText(cluster.clusterArn());

		// Access type
		clusterAccess.clearItems();
		if (!StringUtils.equals(
				"DISABLED",
				cluster.brokerNodeGroupInfo().connectivityInfo().publicAccess().type())) {
			clusterAccess.addItem(PRIVATE_ACCESS);
		} else {
			clusterAccess.addItem(PRIVATE_ACCESS);
			clusterAccess.addItem(PUBLIC_ACCESS);
		}

		// Auth
		clusterAuth.clearItems();
		final String clientBroker = cluster.encryptionInfo().encryptionInTransit().clientBrokerAsString();
		if (clientBroker.endsWith("PLAINTEXT")) {
			// TLS_PLAINTEXT or PLAINTEXT
			clusterAuth.addItem(
					"NONE:" +
					"Without both Authentication and In-transit encryption");
		}
		if (StringUtils.startsWithIgnoreCase("TLS", clientBroker)) {
			final ClientAuthentication clientAuth = cluster.clientAuthentication();
			if (clientAuth.unauthenticated() != null &&
					clientAuth.unauthenticated().enabled()) {
				clusterAuth.addItem(
						"SSL:" +
						"Without Authentication and with TLS(SSL) encrypted traffic between broker and connector");
			}
			if (clientAuth.sasl() != null &&
					clientAuth.sasl().iam() != null &&
					clientAuth.sasl().iam().enabled()) {
				clusterAuth.addItem(
						"IAM:" +
						"With SASL IAM Authentication and with TLS(SSL) encrypted traffic between broker and connector");
			}
			if (clientAuth.sasl() != null &&
					clientAuth.sasl().scram() != null &&
					clientAuth.sasl().scram().enabled()) {
				clusterAuth.addItem(
						"SCRAM:" +
						"With SASL SCRAM Authentication and with TLS(SSL) encrypted traffic between broker and connector");
			}
			if (clientAuth.tls() != null &&
					clientAuth.tls().enabled()) {
				clusterAuth.addItem(
						"MUTUAL_TLS:" +
						"With Mutual TLS/SSL Authentication and with TLS(SSL) encrypted traffic between broker and connector");
			}
		}		

	}

	public String bootstrapServers(final String arn, final int port) throws IOException, KafkaException {
		final StringBuilder sbServers = new StringBuilder(256);
		boolean first = true;
		ListNodesRequest nodesRequest = ListNodesRequest.builder()
				.clusterArn(arn)
				.build();
		ListNodesResponse nodesResponse = null;
		try {
			nodesResponse = kafkaClient.listNodes(nodesRequest);
		} catch(KafkaException ke) {
			LOGGER.error(ke.getMessage());
			throw ke;
		}
		if (nodesResponse != null && nodesResponse.hasNodeInfoList()) {
			for (NodeInfo nodeInfo : nodesResponse.nodeInfoList()) {
				if (nodeInfo.nodeType().equals(NodeType.BROKER)) {
					for (String ep: nodeInfo.brokerNodeInfo().endpoints()) {
						if (first) {
							first = false;
						} else {
							sbServers.append(",");
						}
						sbServers.append(ep);
						sbServers.append(":");
						sbServers.append(port);
					}
				}
			}
		} else {
			LOGGER.error("Unable to get endpoints for ARN '{}'", arn);
			throw new IOException("Unable to get cluster endpoints!");
		}
		return sbServers.toString();
	}

}
