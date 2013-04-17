package com.piusvelte.hydra.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class HydraClient {

	public static final String PARAM_DATABASE = "database";
	public static final String PARAM_TOKEN = "token";
	public static final String PARAM_VALUES = "values";
	public static final String PARAM_COLUMNS = "columns";
	public static final String PARAM_SELECTION = "selection";
	public static final String PARAM_ARGUMENTS = "arguments";
	public static final String PARAM_COMMAND = "command";
	public static final String PARAM_QUEUEABLE = "queueable";
	public static final String ACTION_ABOUT = "about";
	public static final String ACTION_QUERY = "query";
	public static final String ACTION_INSERT = "insert";
	public static final String ACTION_UPDATE = "update";
	public static final String ACTION_EXECUTE = "execute";
	public static final String ACTION_SUBROUTINE = "subroutine";
	public static final String ACTION_DELETE = "delete";

	private static final String Sresult = "result";
	private static final String Salias = "alias";
	private static final String Stype = "type";
	private static final String Shost = "host";
	private static final String Sport = "port";
	public static final String[] DATABASE_ATTRS = new String[]{Salias, Stype, Shost, Sport, PARAM_DATABASE};

	private String scheme = "";
	private String host = "";
	private static final String CONTEXT = "/Hydra";
	private static final String PATH_AUTH = "/auth";
	private static final String PATH_API = "/api";

	private String token = "";
	private String passphrase = null;

	public HydraClient(String host, String passphrase, boolean isSSL, String token) {
		this.host = host;
		if (isSSL)
			scheme = "https";
		else
			scheme = "http";
		this.passphrase = passphrase;
		this.token = token;
	}

	// this can be run as a command line client
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Scanner user_input = new Scanner(System.in);
		String host = null;
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
		System.out.print("Passphrase:");
		String passphrase = user_input.nextLine();
		System.out.print("Token:");
		String token = user_input.nextLine();
		System.out.print("Use SSL? (true,false):");
		String use_sslStr = user_input.nextLine();
		use_ssl = Boolean.parseBoolean(use_sslStr);
		HydraClient hydraClient = new HydraClient(host, passphrase, use_ssl, token);

		// get a token
		String unauthorizedToken;
		try {
			unauthorizedToken = hydraClient.getUnauthorizedToken();
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		try {
			token = hydraClient.authorizeToken(unauthorizedToken);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		if (hydraClient != null) {
			System.out.print("Action, or null to exit:");
			action = user_input.nextLine();
			while ((action != null) && (action.length() > 0)) {
				System.out.print("Database:");
				database = user_input.nextLine();
				if ((database != null) && (database.length() > 0)) {
					System.out.print("Target:");
					target = user_input.nextLine();
					if ((target != null) && (target.length() > 0)) {
						System.out.println("Enter columns...");
						System.out.print("column:");
						String column = user_input.nextLine();
						while ((column != null) && (column.length() > 0)) {
							if (column.equals("\"\""))
								columns.add("");
							else
								columns.add(column);
							System.out.print("column:");
							column = user_input.nextLine();
						}
						System.out.println("Enter values...");
						System.out.print("value:");
						String value = user_input.nextLine();
						while ((value != null) && (value.length() > 0)) {
							if (value.equals("\"\""))
								values.add("");
							else
								values.add(value);
							System.out.print("value:");
							value = user_input.nextLine();
						}
						System.out.print("Selection:");
						selection = user_input.nextLine();
						System.out.print("Queueable (true,false):");
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
						} catch (Exception e) {
							e.printStackTrace();
						}
					} else {
						try {
							HashMap<String, String> databaseInfo = hydraClient.getDatabase(database);
							Set<String> keys = databaseInfo.keySet();
							for (String key : keys)
								System.out.println(key + ": " + databaseInfo.get(key));
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				} else if (ACTION_EXECUTE.equals(action)) {
					try {
						JSONObject response = hydraClient.execute(database, target, queueable);
						if (response.containsKey("result")) {
							JSONArray vmValues = (JSONArray) response.get("result");
							for (int i = 0, l = vmValues.size(); i < l; i++) {
								JSONArray svmValues = (JSONArray) vmValues.get(i);
								for (int i2 = 0, l2 = svmValues.size(); i2 < l2; i2++)
									System.out.println(svmValues.get(i2));
							}
						} else
							System.out.println("no result");
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (ACTION_SUBROUTINE.equals(action)) {
					String[] valuesArr = new String[values.size()];
					for (int i = 0, l = valuesArr.length; i < l; i++)
						valuesArr[i] = values.get(i);
					try {
						JSONObject response = hydraClient.subroutine(database, target, valuesArr, queueable);
						if (response.containsKey("result")) {
							JSONArray vmValues = (JSONArray) response.get("result");
							for (int i = 0, l = vmValues.size(); i < l; i++) {
								JSONArray svmValues = (JSONArray) vmValues.get(i);
								for (int i2 = 0, l2 = svmValues.size(); i2 < l2; i2++)
									System.out.println(svmValues.get(i2));
							}
						} else
							System.out.println("no result");
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (ACTION_QUERY.equals(action)) {
					String[] columnsArr = new String[columns.size()];
					for (int i = 0, l = columnsArr.length; i < l; i++)
						columnsArr[i] = columns.get(i);
					try {
						JSONObject response = hydraClient.query(database, target, columnsArr, selection, queueable);
						if (response.containsKey("result")) {
							JSONArray rowsJArr = (JSONArray) response.get("result");
							for (int r = 0, rl = rowsJArr.size(); r < rl; r++) {
								JSONObject columnsObj = (JSONObject) rowsJArr.get(r);
								Set<String> columnsNames = columnsObj.keySet();
								for (String columnName : columnsNames)
									System.out.println(columnName + "= " + ((String) columnsObj.get(columnName)));
							}
						}
					} catch (Exception e) {
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
					} catch (Exception e) {
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
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				System.out.println("Action, or null to exit:");
				action = user_input.nextLine();
			}
		}
	}

	private HttpClient httpClient = new DefaultHttpClient();
	private JSONParser jsonParser = new JSONParser();
	
	private String getHttpEntity(HttpUriRequest request) throws Exception {
		HttpResponse response = httpClient.execute(request);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			InputStream is = entity.getContent();
			ByteArrayOutputStream content = new ByteArrayOutputStream();
			byte[] sBuffer = new byte[512];
			int readBytes = 0;
			while ((readBytes = is.read(sBuffer)) != -1)
				content.write(sBuffer, 0, readBytes);
			return new String(content.toByteArray());
		} else
			throw new Exception("response is empty");
	}

	public String getUnauthorizedToken() throws Exception {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(scheme).setHost(host).setPath(CONTEXT + PATH_AUTH);
		URI uri = builder.build();
		HttpGet httpGet = new HttpGet(uri);
		JSONObject result = (JSONObject) jsonParser.parse(getHttpEntity(httpGet));
		return (String) result.get(Sresult);
	}

	public String authorizeToken(String token) throws Exception {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(scheme).setHost(host).setPath(CONTEXT + PATH_AUTH).setParameter(PARAM_TOKEN, getHash64(token + passphrase));
		URI uri = builder.build();
		HttpGet httpGet = new HttpGet(uri);
		JSONObject result = (JSONObject) jsonParser.parse(getHttpEntity(httpGet));
		return (String) result.get(Sresult);
	}

	private static String getHash64(String in) {
		String out = null;
		MessageDigest md;
		try {
			md = MessageDigest.getInstance("SHA-256");
			md.update(in.getBytes("UTF-8"));
			out = new BigInteger(1, md.digest()).toString(16);
			StringBuffer hexString = new StringBuffer();
			byte[] hash = md.digest();
			for (byte b : hash) {
				if ((0xFF & b) < 0x10)
					hexString.append("0" + Integer.toHexString((0xFF & b)));
				else
					hexString.append(Integer.toHexString(0xFF & b));
			}
			out = hexString.toString();
			if (out.length() > 64)
				return out.substring(0, 64);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return out;
	}
	
	public URI buildURI(String database, String entity, String[] columns, String[] values, String selection, boolean queueable) throws ParseException, Exception {
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme(scheme)
		.setHost(host)
		.setPath(CONTEXT + PATH_API + (database != null ? "/" + database + (entity != null ? "/" + entity : "") : ""))
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		if (columns != null) {
			for (int i = 0; i < columns.length; i++)
				builder.setParameter("columns[" + i + "]", URLEncoder.encode(columns[i], "UTF-8"));
		}
		if (values != null) {
			for (int i = 0; i < values.length; i++)
				builder.setParameter("values[" + i + "]", URLEncoder.encode(values[i], "UTF-8"));
		}
		if (selection != null)
			builder.setParameter(PARAM_SELECTION, URLEncoder.encode(selection, "UTF-8"));
		return builder.build();
	}
	
	public URI buildURI(String database, String entity, String[] arguments, boolean queueable) throws ParseException, Exception {
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme(scheme)
		.setHost(host)
		.setPath(CONTEXT + PATH_API + (database != null ? "/" + database + (entity != null ? "/" + entity : "") : ""))
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		if (arguments != null) {
			for (int i = 0; i < arguments.length; i++)
				builder.setParameter("arguments[" + i + "]", URLEncoder.encode(arguments[i], "UTF-8"));
		}
		return builder.build();
	}
	
	public URI buildURI(String database, String command, boolean queueable) throws ParseException, Exception {
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme(scheme)
		.setHost(host)
		.setPath(CONTEXT + PATH_API + (database != null ? "/" + database : ""))
		.setParameter(PARAM_COMMAND, URLEncoder.encode(command, "UTF-8"))
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		return builder.build();
	}

	@SuppressWarnings("unchecked")
	public String[] getDatabases() throws Exception {
		JSONObject jobj = query(null, null, null, null, false);
		JSONArray arr = (JSONArray) jobj.get(Sresult);
		String[] databases;
		if (arr != null) {
			databases = new String[arr.size()];
			for (int i = 0, l = databases.length; i < l; i++)
				databases[i] = (String) arr.get(i);
		} else
			databases = new String[0];
		return databases;
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, String> getDatabase(String database) throws Exception {
		JSONObject jobj = query(database, null, null, null, false);
		HashMap<String, String> db = new HashMap<String, String>();
		JSONObject dbObj = (JSONObject) jobj.get(Sresult);
		for (String attr : DATABASE_ATTRS) {
			if (dbObj.containsKey(attr))
				db.put(attr, (String) dbObj.get(attr));
			else
				db.put(attr, null);
		}
		return db;
	}

	@SuppressWarnings("unchecked")
	public JSONObject execute(String database, String command, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpPost(buildURI(database, command, queueable))));
	}

	@SuppressWarnings("unchecked")
	public JSONObject query(String database, String entity, String[] columns, String selection, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpGet(buildURI(database, entity, columns, null, selection, queueable))));
	}

	@SuppressWarnings("unchecked")
	public JSONObject update(String database, String entity, String[] columns, String[] values, String selection, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpPut(buildURI(database, entity, columns, values, selection, queueable))));
	}

	@SuppressWarnings("unchecked")
	public JSONObject insert(String database, String entity, String[] columns, String[] values, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpPost(buildURI(database, entity, columns, values, null, queueable))));
	}

	@SuppressWarnings("unchecked")
	public JSONObject delete(String database, String entity, String selection, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpDelete(buildURI(database, entity, null, null, selection, queueable))));
	}

	@SuppressWarnings("unchecked")
	public JSONObject subroutine(String database, String entity, String[] values, boolean queueable) throws Exception {
		return (JSONObject) jsonParser.parse(getHttpEntity(new HttpPost(buildURI(database, entity, values, queueable))));
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

}
