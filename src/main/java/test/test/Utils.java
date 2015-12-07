package test.test;

public class Utils {
	public static final String zkConnString = "192.168.1.141:2181,192.168.1.140:2181,192.168.1.172:2181";
	public static final String KAFKA_TOPIC_EXTRACT = "orders-extract";

	public static void waitForSeconds(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
		}
	}

	public static void waitForMillis(long milliseconds) {
		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {
		}
	}

	public static String trim(String str) {
		try {
			return str.substring(1, str.length() - 1);
		} catch (Exception ex) {
		}
		return "";
	}

	public static int parseInt(String i, int def) {
		try {
			return Integer.parseInt(i);
		} catch (NumberFormatException e) {
		}
		return def;
	}
}
