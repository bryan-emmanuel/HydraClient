package com.figsolutions.hydraclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HydraClient {

	public static final String PARAM_HMAC = "hmac";
	public static final String PARAM_SALT = "salt";
	public static final String PARAM_CHALLENGE = "challenge";
	public static final String PARAM_DATABASE = "database";
	public static final String PARAM_VALUES = "values";
	public static final String PARAM_COLUMNS = "columns";
	public static final String PARAM_SELECTION = "selection";
	public static final String PARAM_TARGET = "target";
	public static final String PARAM_ACTION = "action";
	public static final String PARAM_QUEUEABLE = "queueable";
	public static final String ACTION_ABOUT = "about";
	public static final String ACTION_QUERY = "query";
	public static final String ACTION_INSERT = "insert";
	public static final String ACTION_UPDATE = "update";
	public static final String ACTION_EXECUTE = "execute";
	public static final String ACTION_SUBROUTINE = "subroutine";
	public static final String ACTION_DELETE = "delete";

	private String mHost = null;
	private int mPort;
	private String mPassphrase = null;
	private static final String Sresult = "";
	private static final String Salias = "alias";
	private static final String Stype = "type";
	private static final String Shost = "host";
	private static final String Sport = "port";
	public static final String[] DATABASE_ATTRS = new String[]{Salias, Stype, Shost, Sport, PARAM_DATABASE};

	private Socket mSocket = null;
	private InputStream mInStream = null;
	private OutputStream mOutStream = null;
	private BufferedReader mReader = null;
	private JSONParser mJSONParser = new JSONParser();
	private String mSalt = null;
	private String mChallenge = null;
	private boolean mUse_SSL = false;

	public HydraClient(String host, int port, String passphrase, boolean use_ssl) {
		mHost = host;
		mPort = port;
		mPassphrase = passphrase;
		mUse_SSL = use_ssl;
	}

	// this can be run as a command line client
	public static void main(String[] args) {
		Scanner user_input = new Scanner(System.in);
		String host = null;
		int port = 0;
		String passphrase = null;
		boolean use_ssl = false;
		String action = null;
		String database = null;
		String target = null;
		ArrayList<String> columns = new ArrayList<String>();
		ArrayList<String> values = new ArrayList<String>();
		String selection = null;
		boolean queueable = false;
		System.out.print("Host, or null to exit:");
		host = user_input.nextLine();
		if ((host == null) || (host.length() == 0))
			return;
		System.out.println("Port, or null to exit:");
		String portStr = user_input.nextLine();
		if ((portStr == null) || (portStr.length() == 0))
			return;
		port = Integer.parseInt(portStr);
		System.out.println("Passphrase:");
		passphrase = user_input.nextLine();
		System.out.println("Use SSL? (true,false):");
		String use_sslStr = user_input.nextLine();
		use_ssl = Boolean.parseBoolean(use_sslStr);
		HydraClient hydraClient = new HydraClient(host, port, passphrase, use_ssl);
		try {
			hydraClient.open();
		} catch (IOException e) {
			hydraClient = null;
			e.printStackTrace();
		} catch (ParseException e) {
			hydraClient = null;
			e.printStackTrace();
		}
		if (hydraClient != null) {
			System.out.println("Action, or null to exit:");
			action = user_input.nextLine();
			while ((action != null) && (action.length() > 0)) {
				System.out.println("Database:");
				database = user_input.nextLine();
				if ((database != null) && (database.length() > 0)) {
					System.out.println("Target:");
					target = user_input.nextLine();
					if ((target != null) && (target.length() > 0)) {
						System.out.println("Enter columns...");
						System.out.println("column:");
						String column = user_input.nextLine();
						while ((column != null) && (column.length() > 0)) {
							if (column.equals("\"\""))
								columns.add("");
							else
								columns.add(column);
							System.out.println("column:");
							column = user_input.nextLine();
						}
						System.out.println("Enter values...");
						System.out.println("value:");
						String value = user_input.nextLine();
						while ((value != null) && (value.length() > 0)) {
							if (value.equals("\"\""))
								values.add("");
							else
								values.add(column);
							System.out.println("value:");
							value = user_input.nextLine();
						}
						System.out.println("Selection:");
						selection = user_input.nextLine();
						System.out.println("Queueable (true,false):");
						String queueableStr = user_input.nextLine();
						queueable = Boolean.parseBoolean(queueableStr);
					}
				}
				if (ACTION_ABOUT.equals(action)) {
					if ((database == null) || (database.length() == 0)) {
						try {
							String[] databases = hydraClient.getDatabases();
							for (String db : databases)
								System.out.println("database: " + db);
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ParseException e) {
							e.printStackTrace();
						}
					} else {
						try {
							HashMap<String, String> databaseInfo = hydraClient.getDatabase(database);
							Set<String> keys = databaseInfo.keySet();
							for (String key : keys)
								System.out.println(key + ": " + databaseInfo.get(key));
						} catch (NoSuchAlgorithmException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
				} else if (ACTION_EXECUTE.equals(action)) {
					try {
						JSONObject response = hydraClient.execute(database, target, queueable);
						if (response.containsKey("result"))
							System.out.println(response.get("result"));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (ACTION_SUBROUTINE.equals(action)) {
					String[] valuesArr = new String[values.size()];
					for (int i = 0, l = valuesArr.length; i < l; i++)
						valuesArr[i] = values.get(i);
					try {
						JSONObject response = hydraClient.subroutine(database, target, valuesArr, queueable);
						if (response.containsKey("result"))
							System.out.println(response.get("result"));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (ACTION_QUERY.equals(action)) {
					String[] columnsArr = new String[columns.size()];
					for (int i = 0, l = columnsArr.length; i < l; i++)
						columnsArr[i] = columns.get(i);
					try {
						JSONObject response = hydraClient.query(database, target, columnsArr, selection, queueable);
						if (response.containsKey("result"))
							System.out.println(response.get("result"));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (ACTION_INSERT.equals(action)) {
					String[] columnsArr = new String[columns.size()];
					for (int i = 0, l = columnsArr.length; i < l; i++)
						columnsArr[i] = columns.get(i);
					String[] valuesArr = new String[values.size()];
					for (int i = 0, l = valuesArr.length; i < l; i++)
						valuesArr[i] = values.get(i);
					try {
						JSONObject response = hydraClient.insert(database, target, columnsArr, valuesArr, queueable);
						if (response.containsKey("result"))
							System.out.println(response.get("result"));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				} else if (ACTION_UPDATE.equals(action)) {
					String[] columnsArr = new String[columns.size()];
					for (int i = 0, l = columnsArr.length; i < l; i++)
						columnsArr[i] = columns.get(i);
					String[] valuesArr = new String[values.size()];
					for (int i = 0, l = valuesArr.length; i < l; i++)
						valuesArr[i] = values.get(i);
					try {
						JSONObject response = hydraClient.update(database, target, columnsArr, valuesArr, selection, queueable);
						if (response.containsKey("result"))
							System.out.println(response.get("result"));
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} catch (ParseException e) {
						e.printStackTrace();
					}
				}
				System.out.println("Action, or null to exit:");
				action = user_input.nextLine();
			}
			try {
				hydraClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean isOpen() {
		return (mOutStream != null) && (mInStream != null);
	}

	public boolean open() throws IOException, ParseException {
		String response = null;
		try {
			if (mUse_SSL) {
				SocketFactory sf = SSLSocketFactory.getDefault();
				mSocket = sf.createSocket(mHost, mPort);
			} else
				mSocket = new Socket(mHost, mPort);
			mInStream = mSocket.getInputStream();
			mOutStream = mSocket.getOutputStream();
			mReader = new BufferedReader(new InputStreamReader(mInStream));
			response = mReader.readLine();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (response == null)
			throw new IOException("no response read");
		JSONObject jsonResponse = (JSONObject) mJSONParser.parse(response);
		setCredentials(jsonResponse);
		if ((mSalt == null) || (mChallenge == null))
			throw new IOException("salt or challenge not set");
		return true;
	}

	public boolean close() throws IOException {
		if (mSocket != null) {
			if (mInStream != null)
				mInStream.close();
			if (mOutStream != null)
				mOutStream.close();
			mSocket.close();
		}
		return true;
	}

	public static String getHashString(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(str.getBytes("UTF-8"));
		StringBuffer hexString = new StringBuffer();
		byte[] hash = md.digest();
		for (byte b : hash) {
			if ((0xFF & b) < 0x10)
				hexString.append("0" + Integer.toHexString((0xFF & b)));
			else
				hexString.append(Integer.toHexString(0xFF & b));
		}
		return hexString.toString();
	}

	@SuppressWarnings("unchecked")
	public String[] getDatabases() throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		String[] databases = new String[0];
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_ABOUT);
		JSONObject jobj = getResult(requestObj);
		JSONArray arr = (JSONArray) jobj.get(Sresult);
		if (arr != null) {
			databases = new String[arr.size()];
			for (int i = 0, l = databases.length; i < l; i++)
				databases[i] = (String) arr.get(i);
		}
		return databases;
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, String> getDatabase(String database) throws NoSuchAlgorithmException, IOException, ParseException {
		HashMap<String, String> db = new HashMap<String, String>();
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_ABOUT);
		requestObj.put(PARAM_DATABASE, database);
		JSONObject jobj = getResult(requestObj);
		JSONObject dbObj = (JSONObject) jobj.get(Sresult);
		// set attrs
		for (String attr : DATABASE_ATTRS) {
			if (dbObj.containsKey(attr))
				db.put(attr, (String) dbObj.get(attr));
			else
				db.put(attr, null);
		}
		return db;
	}

	@SuppressWarnings("unchecked")
	public JSONObject execute(String database, String target, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_EXECUTE);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	@SuppressWarnings("unchecked")
	public JSONObject query(String database, String target, String[] columns, String selection, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_QUERY);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		JSONArray columnsArr = new JSONArray();
		for (String c : columns)
			columnsArr.add(c);
		requestObj.put(PARAM_COLUMNS, columnsArr);
		requestObj.put(PARAM_SELECTION, selection);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	@SuppressWarnings("unchecked")
	public JSONObject update(String database, String target, String[] columns, String[] values, String selection, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_UPDATE);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		JSONArray columnsArr = new JSONArray();
		for (String c : columns)
			columnsArr.add(c);
		requestObj.put(PARAM_COLUMNS, columnsArr);
		JSONArray valuesArr = new JSONArray();
		for (String v : values)
			valuesArr.add(v);
		requestObj.put(PARAM_VALUES, valuesArr);
		requestObj.put(PARAM_SELECTION, selection);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	@SuppressWarnings("unchecked")
	public JSONObject insert(String database, String target, String[] columns, String[] values, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_INSERT);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		JSONArray columnsArr = new JSONArray();
		for (String c : columns)
			columnsArr.add(c);
		requestObj.put(PARAM_COLUMNS, columnsArr);
		JSONArray valuesArr = new JSONArray();
		for (String v : values)
			valuesArr.add(v);
		requestObj.put(PARAM_VALUES, valuesArr);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	@SuppressWarnings("unchecked")
	public JSONObject delete(String database, String target, String selection, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_DELETE);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		requestObj.put(PARAM_SELECTION, selection);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	@SuppressWarnings("unchecked")
	public JSONObject subroutine(String database, String target, String[] values, boolean queueable) throws UnknownHostException, IOException, ParseException, NoSuchAlgorithmException {
		JSONObject requestObj = new JSONObject();
		requestObj.put(PARAM_ACTION, ACTION_SUBROUTINE);
		requestObj.put(PARAM_DATABASE, database);
		requestObj.put(PARAM_TARGET, target);
		JSONArray valuesArr = new JSONArray();
		for (String v : values)
			valuesArr.add(v);
		requestObj.put(PARAM_VALUES, valuesArr);
		requestObj.put(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return getResult(requestObj);
	}

	private void setCredentials(JSONObject jobj) {
		if (jobj.containsKey(PARAM_SALT))
			mSalt = (String) jobj.get(PARAM_SALT);
		if (jobj.containsKey(PARAM_CHALLENGE))
			mChallenge = (String) jobj.get(PARAM_CHALLENGE);
	}

	@SuppressWarnings("unchecked")
	public JSONObject getResult(JSONObject request) throws IOException, ParseException, NoSuchAlgorithmException {
		request.put(PARAM_HMAC, getHMAC(request));
		mOutStream.write((request.toJSONString() + "\n").getBytes());
		String response = mReader.readLine();
		if (response == null)
			throw new IOException("no response read");
		JSONObject jsonResponse = (JSONObject) mJSONParser.parse(response);
		setCredentials(jsonResponse);
		if (!jsonResponse.containsKey(Sresult))
			throw new IOException("no result");
		return jsonResponse;
	}

	String[] parseArray(JSONArray jarr) {
		String[] sArr;
		if (jarr == null)
			sArr = new String[0];
		else
			sArr = new String[jarr.size()];
		for (int i = 0, l = sArr.length; i < l; i++)
			sArr[i] = (String) jarr.get(i);
		return sArr;
	}

	String getHMAC(JSONObject request) throws NoSuchAlgorithmException, UnsupportedEncodingException {
		String saltedPassphrase = getHashString(mSalt + mPassphrase);
		if (saltedPassphrase.length() > 64)
			saltedPassphrase = saltedPassphrase.substring(0, 64);
		String requestAuth = "";
		String[] params = new String[]{PARAM_ACTION, PARAM_DATABASE, PARAM_TARGET};
		for (String p : params) {
			String value = (String) request.get(p);
			if (value == null)
				value = "";
			requestAuth += value;
		}
		String[] columns = parseArray((JSONArray) request.get(PARAM_COLUMNS));
		for (String s : columns)
			requestAuth += s;
		String[] values = parseArray((JSONArray) request.get(PARAM_VALUES));
		for (String s : values)
			requestAuth += s;
		params = new String[]{PARAM_SELECTION, PARAM_QUEUEABLE};
		for (String p : params) {
			String value = (String) request.get(p);
			if (value == null)
				value = "";
			requestAuth += value;
		}
		String hmac = getHashString(requestAuth + mChallenge + saltedPassphrase);
		if (hmac.length() > 64)
			hmac = hmac.substring(0, 64);
		return hmac;
	}

}
