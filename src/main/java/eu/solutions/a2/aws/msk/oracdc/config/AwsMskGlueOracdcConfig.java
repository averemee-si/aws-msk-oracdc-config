package eu.solutions.a2.aws.msk.oracdc.config;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.Button;
import com.googlecode.lanterna.gui2.ComboBox;
import com.googlecode.lanterna.gui2.DefaultWindowManager;
import com.googlecode.lanterna.gui2.EmptySpace;
import com.googlecode.lanterna.gui2.GridLayout;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.RadioBoxList;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.dialogs.FileDialogBuilder;
import com.googlecode.lanterna.gui2.dialogs.MessageDialogBuilder;
import com.googlecode.lanterna.gui2.dialogs.MessageDialogButton;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;

import software.amazon.awssdk.services.kafka.model.KafkaException;
import software.amazon.awssdk.utils.StringUtils;

public class AwsMskGlueOracdcConfig {

	private static final Logger LOGGER = LoggerFactory.getLogger(AwsMskGlueOracdcConfig.class);
	private static final TerminalSize LABEL_SIZE = new TerminalSize(20, 1);
	private static final TerminalSize SINGLE_ROW = new TerminalSize(54, 1);
	private static final TerminalSize DOUBLE_ROW = new TerminalSize(54, 2);
	private static final EmptySpace EMPTY_CELL= new EmptySpace(new TerminalSize(1,1));
	private static final Pattern PORT_NUMBER = Pattern.compile("[0-9]*");
	private static final Pattern KAFKA_TOPIC = Pattern.compile("[a-zA-Z0-9._\\-]*");
	private static final String LBL_NEXT = "Next";
	private static final String LBL_FINISH = "Finish";
	private static final String RETYPE_PWD = "Retype password";
	private static final String WORKER_PORT = "8083";

	final AwsObjects clusters;
	final String regionName;

	final MultiWindowTextGUI gui;
	final Window window ;

	final Panel mainPanel = new Panel();
	final ComboBox<String> cbClusters = new ComboBox<>();
	final Label lblClusterArn = new Label("");
	final RadioBoxList<String> rblClusterAccess = new RadioBoxList<>();
	final ComboBox<String> cbAuth = new ComboBox<>();
	final TextBox tbRegistryName =  new TextBox();
	final TextBox tbWorkerPort = new TextBox();
	final TextBox tbGroupId = new TextBox();
	final TextBox tbOffsetStorage = new TextBox();
	final TextBox tbConfigStorage = new TextBox();
	final TextBox tbStatusStorage = new TextBox();
	final Button btnNext = new Button(LBL_NEXT);

	final Panel workerPanel = new Panel();
	final Label lblScramUserName = new Label("SCRAM Username");
	final TextBox tbScramUserName = new TextBox();
	final Label lblScramPassword = new Label("SCRAM Password");
	final TextBox tbScramPassword = new TextBox();
	final Label lblScramPassword2 = new Label(RETYPE_PWD);
	final TextBox tbScramPassword2 = new TextBox();
	final Button btnTrustStoreLocation = new Button("Truststore");
	final Label lblTrustStoreLocation = new Label("");
	final Button btnKeyStoreLocation = new Button("Keystore");
	final Label lblKeyStoreLocation = new Label("");
	final Label lblKeyStorePassword = new Label("Keystore Password");
	final TextBox tbKeyStorePassword = new TextBox();
	final Label lblKeyStorePassword2 = new Label(RETYPE_PWD);
	final TextBox tbKeyStorePassword2 = new TextBox();
	final Label lblKeyPassword = new Label("Key Password");
	final TextBox tbKeyPassword = new TextBox();
	final Label lblKeyPassword2 = new Label(RETYPE_PWD);
	final TextBox tbKeyPassword2 = new TextBox();

	AwsMskGlueOracdcConfig(final String regionName, final AwsObjects clusters) {
		this.regionName = regionName;
		Screen screen = null;
		try {
			// Setup terminal and screen layers
			Terminal terminal = new DefaultTerminalFactory().createTerminal();
			screen = new TerminalScreen(terminal);
			screen.startScreen();
		
		} catch (IOException ioe) {
			LOGGER.error(ioe.getMessage());
			final StringWriter sw = new StringWriter();
			final PrintWriter pw = new PrintWriter(sw);
			ioe.printStackTrace(pw);
			LOGGER.error(sw.toString());
			System.exit(1);
		}
		gui = new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(TextColor.ANSI.BLUE));
		window = new BasicWindow("Worker configuration in '" + regionName + "'region");

		this.clusters = clusters;
	}

	private void setUpMainConfigPanel() {
		mainPanel.setLayoutManager(new GridLayout(2));

		// Row 1
		final Label lblChooseCluster = new Label("Choose MSK cluster");
		lblChooseCluster.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblChooseCluster);
		
		clusters.getClustersList(cbClusters);
		cbClusters.setPreferredSize(SINGLE_ROW);
		mainPanel.addComponent(cbClusters);

		// Row 2
		final Label lblArnLabel = new Label("Cluster ARN");
		lblArnLabel.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblArnLabel);
		lblClusterArn.setPreferredSize(DOUBLE_ROW);
		mainPanel.addComponent(lblClusterArn);

		//Row 3
		final Label lblClusterAccess = new Label("Cluster Access");
		lblClusterAccess.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblClusterAccess);
		rblClusterAccess.setPreferredSize(DOUBLE_ROW);
		mainPanel.addComponent(rblClusterAccess);

		// Row 4
		final Label lblClusterAuth = new Label("AuthN/AuthZ to use");
		lblClusterAuth.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblClusterAuth);
		cbAuth.setPreferredSize(SINGLE_ROW);
		mainPanel.addComponent(cbAuth);

		//Row 5
		final Label lblRegistryName = new Label("AWS Glue Registry");
		lblRegistryName.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblRegistryName);
		tbRegistryName.setPreferredSize(SINGLE_ROW);
		tbRegistryName.setValidationPattern(KAFKA_TOPIC);
		tbRegistryName.setText("default");
		mainPanel.addComponent(tbRegistryName);

		//Row 6
		final Label lblWorkerPort = new Label("rest.port");
		lblWorkerPort.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblWorkerPort);
		tbWorkerPort.setPreferredSize(SINGLE_ROW);
		tbWorkerPort.setValidationPattern(PORT_NUMBER);
		tbWorkerPort.setText(WORKER_PORT);
		mainPanel.addComponent(tbWorkerPort);

		//Row 7
		final Label lblGroupId = new Label("group.id");
		lblGroupId.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblGroupId);
		tbGroupId.setPreferredSize(SINGLE_ROW);
		tbGroupId.setValidationPattern(KAFKA_TOPIC);
		mainPanel.addComponent(tbGroupId);

		//Row 8
		final Label lblOffsetStorage = new Label("offset.storage.topic");
		lblOffsetStorage.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblOffsetStorage);
		tbOffsetStorage.setPreferredSize(SINGLE_ROW);
		tbOffsetStorage.setValidationPattern(KAFKA_TOPIC);
		mainPanel.addComponent(tbOffsetStorage);

		//Row 9
		final Label lblConfigStorage = new Label("config.storage.topic");
		lblConfigStorage.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblConfigStorage);
		tbConfigStorage.setPreferredSize(SINGLE_ROW);
		tbConfigStorage.setValidationPattern(KAFKA_TOPIC);
		mainPanel.addComponent(tbConfigStorage);

		//Row 10
		final Label lblStatusStorage = new Label("status.storage.topic");
		lblStatusStorage.setPreferredSize(LABEL_SIZE);
		mainPanel.addComponent(lblStatusStorage);
		tbStatusStorage.setPreferredSize(SINGLE_ROW);
		tbStatusStorage.setValidationPattern(KAFKA_TOPIC);
		mainPanel.addComponent(tbStatusStorage);

		mainPanel.addComponent(EMPTY_CELL);
		mainPanel.addComponent(EMPTY_CELL);

		mainPanel.addComponent(new EmptySpace(new TerminalSize(0,0)));
		mainPanel.addComponent(btnNext);
		btnNext.addListener(button -> {
			if (checkMainPanelParameters()) {
				if (StringUtils.equals(LBL_NEXT, button.getLabel())) {
					setMainPanelVisible(false);
					setWorkerPanelVisible(true);
				} else {
					save();
				}
			}
		});

		// Setup dependencies
		if (cbClusters.getSelectedIndex() > -1) {
			clusters.setDependent(cbClusters.getSelectedIndex(), lblClusterArn, rblClusterAccess, cbAuth);
		}
		cbClusters.addListener((selectedIndex, previousSelection, changedByUserInteraction) -> {
			if (selectedIndex > -1 && selectedIndex != previousSelection) {
				clusters.setDependent(selectedIndex, lblClusterArn, rblClusterAccess, cbAuth);
			}
		});
		if (cbAuth.getSelectedIndex() > -1) {
			setAuthDependent(cbAuth.getSelectedIndex());
		}
		cbAuth.addListener((selectedIndex, previousSelection, changedByUserInteraction) -> {
			if (selectedIndex > -1 && selectedIndex != previousSelection) {
				setAuthDependent(selectedIndex);
			}
		});
	}

	private void setMainPanelVisible(final boolean visible) {
		mainPanel.setVisible(visible);
		if (visible) {
			window.setComponent(null);
			window.setComponent(mainPanel);
		}
	}

	private boolean checkMainPanelParameters() {
		if (cbClusters.getSelectedIndex() == -1) {
			new MessageDialogBuilder()
				.setTitle("No MSK Cluster selected")
				.setText("Please choose MSK Cluster!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(cbClusters);
			return false;
		}
		if (rblClusterAccess.getCheckedItemIndex() == -1) {
			new MessageDialogBuilder()
				.setTitle("Cluster access not set")
				.setText("Please choose cluster access!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(rblClusterAccess);
			return false;
		}
		if (cbAuth.getSelectedIndex() == -1) {
			new MessageDialogBuilder()
				.setTitle("No AuthN/AuthZ method selected")
				.setText("Please choose AuthN/AuthZ for MSK Cluster!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(cbAuth);
			return false;
		}
		// Port number
		int port = 0;
		try {
			port = Integer.parseInt(tbWorkerPort.getText());
		} catch(NumberFormatException nfe) {}
		if (port < 1024 || port > 65535) {
			new MessageDialogBuilder()
				.setTitle("Wrong rest.port value")
				.setText("Please choose valid value for rest.port parameter!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(tbWorkerPort);
			return false;
		}
		if (StringUtils.isBlank(tbGroupId.getText())) {
			new MessageDialogBuilder()
				.setTitle("Wrong group.id value")
				.setText("Please enter group.id!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(tbGroupId);
			return false;
		}
		if (StringUtils.isBlank(tbOffsetStorage.getText())) {
			new MessageDialogBuilder()
				.setTitle("Wrong offset.storage.topic value")
				.setText("Please enter offset.storage.topic!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(tbOffsetStorage);
			return false;
		}
		if (StringUtils.isBlank(tbConfigStorage.getText())) {
			new MessageDialogBuilder()
				.setTitle("Wrong config.storage.topic value")
				.setText("Please enter config.storage.topic!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(tbConfigStorage);
			return false;
		}
		if (StringUtils.isBlank(tbStatusStorage.getText())) {
			new MessageDialogBuilder()
				.setTitle("Wrong status.storage.topic value")
				.setText("Please enter status.storage.topic!")
				.addButton(MessageDialogButton.Close)
				.build()
				.showDialog(gui);
			window.setFocusedInteractable(tbStatusStorage);
			return false;
		}
		return true;
	}

	private void setWorkerPanelVisible(final boolean visible) {
		workerPanel.setVisible(visible);
		if (visible) {
			window.setComponent(null);
			final boolean showScram = StringUtils.startsWithIgnoreCase(cbAuth.getSelectedItem(), "SCRAM");
			lblScramUserName.setVisible(showScram);
			tbScramUserName.setVisible(showScram);
			lblScramPassword.setVisible(showScram);
			tbScramPassword.setVisible(showScram);
			lblScramPassword2.setVisible(showScram);
			tbScramPassword2.setVisible(showScram);
			
			btnTrustStoreLocation.setVisible(!showScram);
			lblTrustStoreLocation.setVisible(!showScram);
			btnKeyStoreLocation.setVisible(!showScram);
			lblKeyStoreLocation.setVisible(!showScram);
			lblKeyStorePassword.setVisible(!showScram);
			tbKeyStorePassword.setVisible(!showScram);
			lblKeyStorePassword2.setVisible(!showScram);
			tbKeyStorePassword2.setVisible(!showScram);
			lblKeyPassword.setVisible(!showScram);
			tbKeyPassword.setVisible(!showScram);
			lblKeyPassword2.setVisible(!showScram);
			tbKeyPassword2.setVisible(!showScram);

			window.setComponent(workerPanel);
		}
	}

	private void setUpWorkerConfigPanel() {
		workerPanel.setLayoutManager(new GridLayout(2));

		//Row 1
		lblScramUserName.setPreferredSize(LABEL_SIZE);
		lblScramUserName.setVisible(false);
		workerPanel.addComponent(lblScramUserName);
		tbScramUserName.setPreferredSize(SINGLE_ROW);
		tbScramUserName.setValidationPattern(KAFKA_TOPIC);
		tbScramUserName.setVisible(false);
		workerPanel.addComponent(tbScramUserName);

		//Row 2
		lblScramPassword.setPreferredSize(LABEL_SIZE);
		lblScramPassword.setVisible(false);
		workerPanel.addComponent(lblScramPassword);
		tbScramPassword.setPreferredSize(SINGLE_ROW);
		tbScramPassword.setMask('*');
		tbScramPassword.setVisible(false);
		workerPanel.addComponent(tbScramPassword);

		//Row 3
		lblScramPassword2.setPreferredSize(LABEL_SIZE);
		lblScramPassword2.setVisible(false);
		workerPanel.addComponent(lblScramPassword2);
		tbScramPassword2.setPreferredSize(SINGLE_ROW);
		tbScramPassword2.setMask('*');
		tbScramPassword2.setVisible(false);
		workerPanel.addComponent(tbScramPassword2);

		//Row 4
		btnTrustStoreLocation.setPreferredSize(LABEL_SIZE);
		btnTrustStoreLocation.setVisible(false);
		btnTrustStoreLocation.addListener(button -> {
			File choosen = new FileDialogBuilder()
						.setTitle("Truststore location")
						.setDescription("Choose a file")
						.setActionLabel("Choose")
						.build()
						.showDialog(gui);
			try {
				lblTrustStoreLocation.setText(choosen.getCanonicalPath());
			} catch (IOException ioe) {
				LOGGER.error(ioe.getMessage());
				final StringWriter sw = new StringWriter();
				final PrintWriter pw = new PrintWriter(sw);
				ioe.printStackTrace(pw);
				LOGGER.error(sw.toString());
			}
		});
		workerPanel.addComponent(btnTrustStoreLocation);
		lblTrustStoreLocation.setPreferredSize(DOUBLE_ROW);
		lblTrustStoreLocation.setVisible(false);
		workerPanel.addComponent(lblTrustStoreLocation);

		//Row 5
		btnKeyStoreLocation.setPreferredSize(LABEL_SIZE);
		btnKeyStoreLocation.setVisible(false);
		btnKeyStoreLocation.addListener(button -> {
			File choosen = new FileDialogBuilder()
						.setTitle("Keystore location")
						.setDescription("Choose a file")
						.setActionLabel("Choose")
						.build()
						.showDialog(gui);
			try {
				lblKeyStoreLocation.setText(choosen.getCanonicalPath());
			} catch (IOException ioe) {
				LOGGER.error(ioe.getMessage());
				final StringWriter sw = new StringWriter();
				final PrintWriter pw = new PrintWriter(sw);
				ioe.printStackTrace(pw);
				LOGGER.error(sw.toString());
			}
		});
		workerPanel.addComponent(btnKeyStoreLocation);
		lblKeyStoreLocation.setPreferredSize(DOUBLE_ROW);
		lblKeyStoreLocation.setVisible(false);
		workerPanel.addComponent(lblKeyStoreLocation);

		//Row 6
		lblKeyStorePassword.setPreferredSize(LABEL_SIZE);
		lblKeyStorePassword.setVisible(false);
		workerPanel.addComponent(lblKeyStorePassword);
		tbKeyStorePassword.setPreferredSize(SINGLE_ROW);
		tbKeyStorePassword.setMask('*');
		tbKeyStorePassword.setVisible(false);
		workerPanel.addComponent(tbKeyStorePassword);

		//Row 7
		lblKeyStorePassword2.setPreferredSize(LABEL_SIZE);
		lblKeyStorePassword2.setVisible(false);
		workerPanel.addComponent(lblKeyStorePassword2);
		tbKeyStorePassword2.setPreferredSize(SINGLE_ROW);
		tbKeyStorePassword2.setMask('*');
		tbKeyStorePassword2.setVisible(false);
		workerPanel.addComponent(tbKeyStorePassword2);

		//Row 8
		lblKeyPassword.setPreferredSize(LABEL_SIZE);
		lblKeyPassword.setVisible(false);
		workerPanel.addComponent(lblKeyPassword);
		tbKeyPassword.setPreferredSize(SINGLE_ROW);
		tbKeyPassword.setMask('*');
		tbKeyPassword.setVisible(false);
		workerPanel.addComponent(tbKeyPassword);

		//Row 9
		lblKeyPassword2.setPreferredSize(LABEL_SIZE);
		lblKeyPassword2.setVisible(false);
		workerPanel.addComponent(lblKeyPassword2);
		tbKeyPassword2.setPreferredSize(SINGLE_ROW);
		tbKeyPassword2.setMask('*');
		tbKeyPassword2.setVisible(false);
		workerPanel.addComponent(tbKeyPassword2);

		workerPanel.addComponent(new EmptySpace(LABEL_SIZE));
		workerPanel.addComponent(new EmptySpace(SINGLE_ROW));

		Button btnPrevious = new Button("Previous");
		workerPanel.addComponent(btnPrevious);
		btnPrevious.addListener(button -> {
			setWorkerPanelVisible(false);
			setMainPanelVisible(true);
		});
		Button btnFinish = new Button(LBL_FINISH);
		btnFinish.addListener(button -> {
			if (checkWorkerPanelParameters()) {
				save();
			}
		});
		workerPanel.addComponent(btnFinish);
	}

	private boolean checkWorkerPanelParameters() {
		if (StringUtils.startsWithIgnoreCase(cbAuth.getSelectedItem(), "SCRAM")) {
			if (StringUtils.isBlank(tbScramUserName.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty sasl.scram username")
					.setText("Please enter username for sasl.scram Auth!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbScramUserName);
				return false;
			}
			if (StringUtils.isBlank(tbScramPassword.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty sasl.scram password")
					.setText("Please enter password for sasl.scram Auth!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbScramPassword);
				return false;
			}
			if (StringUtils.isBlank(tbScramPassword2.getText()) ||
					!StringUtils.equals(tbScramPassword.getText(), tbScramPassword2.getText())) {
				new MessageDialogBuilder()
					.setTitle("Wrong validation for sasl.scram password")
					.setText("sasl.scram password and confirmation password do not match!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbScramPassword2);
				return false;
			}
		} else {
			if (StringUtils.isBlank(lblTrustStoreLocation.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty Truststore location")
					.setText("Please choose location of Truststore file!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(btnTrustStoreLocation);
				return false;
			}
			if (StringUtils.isBlank(lblKeyStoreLocation.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty Keystore location")
					.setText("Please choose location of Keystore file!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(btnKeyStoreLocation);
				return false;
			}
			if (StringUtils.isBlank(tbKeyStorePassword.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty Keystore password")
					.setText("Please enter password for Keystore!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbKeyStorePassword);
				return false;
			}
			if (StringUtils.isBlank(tbKeyStorePassword2.getText()) ||
					!StringUtils.equals(tbKeyStorePassword.getText(), tbKeyStorePassword2.getText())) {
				new MessageDialogBuilder()
					.setTitle("Wrong validation for Keystore password")
					.setText("Keystore password and confirmation password do not match!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbKeyStorePassword2);
				return false;
			}
			if (StringUtils.isBlank(tbKeyPassword.getText())) {
				new MessageDialogBuilder()
					.setTitle("Empty Key password")
					.setText("Please enter password for Key!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbKeyPassword);
				return false;
			}
			if (StringUtils.isBlank(tbKeyPassword2.getText()) ||
					!StringUtils.equals(tbKeyPassword.getText(), tbKeyPassword2.getText())) {
				new MessageDialogBuilder()
					.setTitle("Wrong validation for Key password")
					.setText("Key password and confirmation password do not match!")
					.addButton(MessageDialogButton.Close)
					.build()
					.showDialog(gui);
				window.setFocusedInteractable(tbKeyPassword2);
				return false;
			}
		}
		return true;
	}

	private void setAuthDependent(final int selectedIndex) {
		final String auth = cbAuth.getItem(selectedIndex);
		if (StringUtils.startsWithIgnoreCase(auth, "IAM") ||
				StringUtils.startsWithIgnoreCase(auth, "NONE") ||
				StringUtils.startsWithIgnoreCase(auth, "SSL")) {
			btnNext.setLabel(LBL_FINISH);
		} else {
			btnNext.setLabel(LBL_NEXT);
		}
	}

	private void save() {
		final PropertiesManager pm = 
				new PropertiesManager(
						tbGroupId.getText(),
						tbOffsetStorage.getText(),
						tbConfigStorage.getText(),
						tbStatusStorage.getText(),
						tbWorkerPort.getText(),
						regionName,
						tbRegistryName.getText());

		final int brokerPort;
		final boolean privateAccess = StringUtils.equals(
				rblClusterAccess.getCheckedItem(), AwsObjects.PRIVATE_ACCESS); 
		final String auth = cbAuth.getItem(cbAuth.getSelectedIndex());
		if (StringUtils.startsWithIgnoreCase(auth, "IAM")) {
			pm.setAuthIam();
			if (privateAccess) {
				// accessType = "PRIVATE"
				brokerPort = 9098;
			} else {
				// accessType = "PUBLIC"
				brokerPort = 9198;
			}
		} else if (StringUtils.startsWithIgnoreCase(auth, "NONE")) {
			pm.setAuthNone();
			brokerPort = 9092;
		} else if (StringUtils.startsWithIgnoreCase(auth, "SSL")) {
			pm.setAuthSsl();
			if (privateAccess) {
				// accessType = "PRIVATE"
				brokerPort = 9094;
			} else {
				// accessType = "PUBLIC"
				brokerPort = 9194;
			}
		} else if (StringUtils.startsWithIgnoreCase(auth, "SCRAM")) {
			pm.setAuthScram(tbScramUserName.getText(), tbScramPassword.getText());
			if (privateAccess) {
				// accessType = "PRIVATE"
				brokerPort = 9096;
			} else {
				// accessType = "PUBLIC"
				brokerPort = 9196;
			}
		} else {
			// MUTUAL_TLS
			pm.setAuthMutualTls(
					lblTrustStoreLocation.getText(),
					lblKeyStoreLocation.getText(),
					tbKeyStorePassword.getText(),
					tbKeyPassword.getText());
			if (privateAccess) {
				// accessType = "PRIVATE"
				brokerPort = 9094;
			} else {
				// accessType = "PUBLIC"
				brokerPort = 9194;
			}
		}
		try {
			pm.setBootstrapServers(
					clusters.bootstrapServers(lblClusterArn.getText(), brokerPort));
			pm.save("/opt/kafka/config/oracdc-distributed.properties");
		} catch (IOException | KafkaException e) {
			LOGGER.error(e.getMessage());
			final StringWriter sw = new StringWriter();
			final PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			LOGGER.error(sw.toString());
			System.exit(1);
		}
		new MessageDialogBuilder()
			.setTitle("Config ready")
			.setText("Please review\n/opt/kafka/config/oracdc-distributed.properties\nand continue with configuration")
			.addButton(MessageDialogButton.Close)
			.build()
			.showDialog(gui);
		System.exit(0);
	}

	public void startGui() {
		setUpMainConfigPanel();
		setMainPanelVisible(true);
		setUpWorkerConfigPanel();
		setWorkerPanelVisible(false);
		gui.addWindowAndWait(window);
	}

	private static void printUsageAndExit() {
		LOGGER.error("\nUsage:\njava {} [--region=<AWS-REGION>]",
				AwsMskGlueOracdcConfig.class.getCanonicalName());
		System.exit(1);
	}

	public static void main(String[] argv) {
		final String log4jConfigFileName = "/opt/kafka/config/connect-log4j.properties";
		Path log4jConfig = Paths.get(log4jConfigFileName);
		if (Files.exists(log4jConfig) && Files.isRegularFile(log4jConfig)) {
			System.setProperty("kafka.logs.dir", "/opt/kafka/logs");
			PropertyConfigurator.configure(log4jConfigFileName);
		} else {
			BasicConfigurator.configure();
			org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
		}

		String awsRegion = null;
		boolean getRegionFromMetadata = true;
		if (argv.length == 1) {
			if (!StringUtils.startsWithIgnoreCase(argv[0], "--region")) {
				printUsageAndExit();
			} else {
				final int equalPos = argv[0].indexOf("=");
				if (equalPos == -1 ) {
					printUsageAndExit();
				} else {
					awsRegion = argv[0]
									.substring(equalPos + 1)
									.trim();
					getRegionFromMetadata = false;
					LOGGER.info("Region {} passed through CLI will be used for oracdc worker configuration", awsRegion);
				}
			}
		} else if (argv.length != 0) {
			printUsageAndExit();
		}

		if (getRegionFromMetadata) {
			try {
				awsRegion = AwsObjects.getRegion();
				LOGGER.info("Region {} from instance metadata will be used for oracdc worker configuration", awsRegion);
			} catch (IOException e) {
				LOGGER.error("Unable to determine region from instance metadata!");
				final StringWriter sw = new StringWriter();
				final PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				LOGGER.error(sw.toString());
				System.exit(1);
			}
		}

		LOGGER.info("Attempting to get list of MSK clusters in {} region", awsRegion);
		AwsObjects clusters = new AwsObjects(awsRegion);
		AwsMskGlueOracdcConfig amgoc = new AwsMskGlueOracdcConfig(awsRegion, clusters);
		amgoc.startGui();
	}

}
