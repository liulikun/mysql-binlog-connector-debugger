import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MysqlBinlogConnectorDebugger {
	private final BinaryLogClient client;

	public MysqlBinlogConnectorDebugger(String host, int port, String user, String password, final String binlogFile, long binlogPosition) throws FileNotFoundException {
		client = new BinaryLogClient(host, port, user, password);
		client.setBinlogFilename(binlogFile);
		client.setBinlogPosition(binlogPosition);

		EventDeserializer eventDeserializer = new EventDeserializer();
		eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO,
			EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
		client.setEventDeserializer(eventDeserializer);

		client.registerLifecycleListener(new BinaryLogClient.LifecycleListener() {
			public void onConnect(BinaryLogClient client) {
			}

			public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("CommunicationFailure: " + ex.getLocalizedMessage());
				ex.printStackTrace();
			}

			public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
				System.out.println("EventDeserializationFailure" + ex.getLocalizedMessage());
				ex.printStackTrace();

			}

			public void onDisconnect(BinaryLogClient client) {
			}
		});

		client.registerEventListener(new BinaryLogClient.EventListener() {
			public void onEvent(Event event) {
				String binlogFilename = client.getBinlogFilename();
				if (!binlogFilename.equals(binlogFile)) {
					try {
						System.out.println("Finished reading " + binlogFile);
						stop();
						System.exit(0);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				System.out.println("POS:" + client.getBinlogPosition() + ", TS:" + event.getHeader().getTimestamp() +
					", EVENT TYPE:" + event.getHeader().getEventType());
			}
		});
	}

	private void start() throws IOException, TimeoutException {
		client.connect(5000);
	}

	private void stop() throws IOException {
		client.disconnect();
	}

	//java -cp mysql-binlog-connector-debugger-0.0.1-jar-with-dependencies.jar host port user password binlog_file binlog_position
	public static void main(String[] args) throws IOException, TimeoutException {
		MysqlBinlogConnectorDebugger debugger = new MysqlBinlogConnectorDebugger(args[0], Integer.parseInt(args[1]),
			args[2], args[3], args[4], Integer.parseInt(args[5]));
		debugger.start();
	}
}
