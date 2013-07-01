/*
 * Hydra
 * Copyright (C) 2012 Bryan Emmanuel
 * 
 * This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Bryan Emmanuel piusvelte@gmail.com
 */
package com.piusvelte.hydra.client;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
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

	private static final String Sresult = "result";
	private static final String Salias = "alias";
	private static final String Stype = "type";
	private static final String Shost = "host";
	private static final String Sport = "port";
	public static final String[] DATABASE_ATTRS = new String[]{Salias, Stype, Shost, Sport, PARAM_DATABASE};

	private String host = "";
	public static final int INVALID_PORT = 0;
	private int port = INVALID_PORT;
	private static final String CONTEXT = "/Hydra";
	private static final String PATH_AUTH = "/auth";
	private static final String PATH_API = "/api";

	private String scheme = "";
	private String token = "";
	private String passphrase = null;

	public HydraClient(String scheme, String host, int port, String passphrase, String token) {
		this.scheme = scheme;
		this.host = host;
		this.port = port;
		this.passphrase = passphrase;
		this.token = getHash64(token + passphrase);
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
			if (response.getStatusLine().getStatusCode() == 200)
				return new String(content.toByteArray());
			else {
				try {
					JSONObject result = (JSONObject) jsonParser.parse(new String(content.toByteArray()));
					if (result.containsKey("errors")) {
						StringBuilder errors = new StringBuilder();
						JSONArray errJSON = (JSONArray) result.get("errors");
						int errSize = errJSON.size();
						if (errSize > 0) {
							errors.append(errJSON.get(0));
							for (int i = 1; i < errSize; i++) {
								errors.append("\n");
								errors.append(errJSON.get(i));
							}
							throw new Exception(errors.toString());
						}
					}
					throw new Exception("unknown error");
				} catch (ParseException e) {
					throw new Exception(new String(content.toByteArray()));
				}
			}
		} else
			throw new Exception("response is empty");
	}

	public void setToken(String token) {
		this.token = getHash64(token + passphrase);
	}

	public String getUnauthorizedToken() throws Exception {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(scheme).setHost(host).setPath(CONTEXT + PATH_AUTH);
		if (port > INVALID_PORT)
			builder.setPort(port);
		URI uri = builder.build();
		HttpGet httpGet = new HttpGet(uri);
		JSONObject result = (JSONObject) jsonParser.parse(getHttpEntity(httpGet));
		return (String) result.get(Sresult);
	}

	public boolean authorizeToken(String token) throws Exception {
		URIBuilder builder = new URIBuilder();
		builder.setScheme(scheme).setHost(host).setPath(CONTEXT + PATH_AUTH).setParameter(PARAM_TOKEN, getHash64(token + passphrase));
		if (port > INVALID_PORT)
			builder.setPort(port);
		URI uri = builder.build();
		HttpGet httpGet = new HttpGet(uri);
		JSONObject result = (JSONObject) jsonParser.parse(getHttpEntity(httpGet));
		return !result.containsKey("errors");
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

	private String packArray(String[] arr) {
		if (arr == null)
			return "";
		if (arr.length == 0)
			return "";
		StringBuilder sb = new StringBuilder();
		try {
			sb.append(URLEncoder.encode(arr[0], "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			sb.append(arr[0]);
		}
		for (int i = 1; i < arr.length; i++) {
			sb.append(",");
			try {
				sb.append(URLEncoder.encode(arr[i], "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				sb.append(arr[0]);
			}
		}
		return sb.toString();
	}

	public URI buildURI(String database, String entity, String[] columns, String[] values, String selection, boolean queueable) throws ParseException, Exception {
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme(scheme)
		.setHost(host)
		.setPath(CONTEXT + PATH_API + (database != null ? "/" + database + (entity != null ? "/" + entity : "") : ""))
		.setParameter(PARAM_TOKEN, token)
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		if (port > INVALID_PORT)
			builder.setPort(port);
		if (columns != null)
			builder.setParameter("columns", packArray(columns));
		if (values != null)
			builder.setParameter("values", packArray(values));
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
		.setParameter(PARAM_TOKEN, token)
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		if (port > INVALID_PORT)
			builder.setPort(port);
		if (arguments != null)
			builder.setParameter("arguments", packArray(arguments));
		return builder.build();
	}

	public URI buildURI(String database, String command, boolean queueable) throws ParseException, Exception {
		URIBuilder builder = new URIBuilder();
		builder
		.setScheme(scheme)
		.setHost(host)
		.setPath(CONTEXT + PATH_API + (database != null ? "/" + database : ""))
		.setParameter(PARAM_TOKEN, token)
		.setParameter(PARAM_COMMAND, URLEncoder.encode(command, "UTF-8"))
		.setParameter(PARAM_QUEUEABLE, Boolean.toString(queueable));
		if (port > INVALID_PORT)
			builder.setPort(port);
		return builder.build();
	}

	@SuppressWarnings("unchecked")
	public String[][] getDatabases() throws Exception {
		return query(null, null, new String[1], null, false);
	}

	@SuppressWarnings("unchecked")
	public String[][] getDatabase(String database) throws Exception {
		return query(database, null, new String[5], null, false);
	}

	@SuppressWarnings("unchecked")
	public String[][] execute(String database, String command, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpPost(buildURI(database, command, queueable))));
	}

	@SuppressWarnings("unchecked")
	public String[][] query(String database, String entity, String[] columns, String selection, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpGet(buildURI(database, entity, columns, null, selection, queueable))), columns.length);
	}

	@SuppressWarnings("unchecked")
	public String[][] update(String database, String entity, String[] columns, String[] values, String selection, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpPut(buildURI(database, entity, columns, values, selection, queueable))));
	}

	@SuppressWarnings("unchecked")
	public String[][] insert(String database, String entity, String[] columns, String[] values, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpPost(buildURI(database, entity, columns, values, null, queueable))));
	}

	@SuppressWarnings("unchecked")
	public String[][] delete(String database, String entity, String selection, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpDelete(buildURI(database, entity, null, null, selection, queueable))));
	}

	@SuppressWarnings("unchecked")
	public String[][] subroutine(String database, String entity, String[] arguments, boolean queueable) throws Exception {
		return parseResult(getHttpEntity(new HttpPost(buildURI(database, entity, arguments, queueable))), arguments.length);
	}

	private String[][] parseResult(String result) throws ParseException {
		JSONArray rows = (JSONArray) ((JSONObject) jsonParser.parse(result)).get("result");
		int colSize = 1;
		for (int r = 0, s = rows.size(); r < s; r++) {
			JSONArray rowData = (JSONArray) rows.get(r);
			if (rowData.size() > colSize)
				colSize = rowData.size();
		}
		return parseResult(rows, colSize);
	}

	private String[][] parseResult(String result, int columnSize) throws ParseException {
		return parseResult((JSONArray) ((JSONObject) jsonParser.parse(result)).get("result"), columnSize);
	}

	private String[][] parseResult(JSONArray rows, int columnSize) throws ParseException {
		String[][] data = new String[rows.size()][columnSize];
		for (int r = 0; r < data.length; r++) {
			JSONArray cols = (JSONArray) rows.get(r);
			for (int c = 0; c < columnSize; c++) {
				if (c < cols.size())
					data[r][c] = (String) cols.get(c);
				else
					data[r][c] = "";
			}
		}
		return data;
	}

}
