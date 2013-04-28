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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Scanner;

import com.piusvelte.hydra.client.HydraClient;

public class HydraTest {
	
	private static HydraClient hydraClient;

	// this can be run as a command line client
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Scanner user_input = new Scanner(System.in);
		String scheme = null;
		String host = null;
		String portStr = null;
		int port = HydraClient.INVALID_PORT;
		String passphrase = null;
		String token = null;
		// try to load a properties file
		System.out.println("looking for properties file...");
		String clientRoot = HydraClient.class.getProtectionDomain().getCodeSource().getLocation().getPath().substring(1);
		String propsFile = clientRoot + ".properties";
		Properties properties = new Properties();
		FileInputStream in = null;
		try {
			in = new FileInputStream(propsFile);
			properties.load(in);
		} catch (FileNotFoundException e) {
			properties = null;
			e.printStackTrace();
		} catch (IOException e) {
			properties = null;
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		String save = null;
		if (properties != null) {
			save = "";
			if (properties.containsKey("scheme"))
				scheme = properties.getProperty("scheme");
			if (properties.containsKey("host"))
				host = properties.getProperty("host");
			if (properties.containsKey("port")) {
				try {
					port = Integer.parseInt(properties.getProperty("port"));
				} catch (NumberFormatException e) {
				}
			}
			if (properties.containsKey("passphrase"))
				passphrase = properties.getProperty("passphrase");
			if (properties.containsKey("token")) {
				token = properties.getProperty("token");
				if (token.length() == 0)
					token = null;
			}
		}
		if ((host == null) || (port == HydraClient.INVALID_PORT) || (passphrase == null)) {
			save = null;
			System.out.println("Unable to load properties.");
			System.out.print("Scheme[http]: ");
			scheme = user_input.nextLine();
			if ((scheme == null) || (scheme.length() == 0))
				scheme = "http";
			System.out.print("Host[localhost]: ");
			host = user_input.nextLine();
			if ((host == null) || (host.length() == 0))
				host = "localhost";			
			System.out.print("Port[80]:");
			portStr = user_input.nextLine();
			port = HydraClient.INVALID_PORT;
			if (portStr != null) {
				try {
					port = Integer.parseInt(portStr);
				} catch (NumberFormatException e) {
					port = HydraClient.INVALID_PORT;
				}
			} else
				port = 80;
			System.out.print("Passphrase:");
			passphrase = user_input.nextLine();
			System.out.print("Attempt to save properties? null to skip:");
			save = user_input.nextLine();
			if (save != null) {
				if (properties == null)
					properties = new Properties();
				properties.setProperty("scheme", scheme);
				properties.setProperty("host", host);
				properties.setProperty("port", Integer.toString(port));
				properties.setProperty("passphrase", passphrase);
				properties.setProperty("token", "");
				FileOutputStream out = null;
				try {
					out = new FileOutputStream(propsFile, false);
					properties.store(out, "");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (out != null) {
						try {
							out.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		hydraClient = new HydraClient(scheme, host, port, passphrase, token);
		if (token == null) {
			token = HydraTest.testTokenAuthorization();
			if (token == null)
				return;
			hydraClient.setToken(token);
			if (save != null) {
				System.out.println("saving the token");
				properties.setProperty("token", token);
				FileOutputStream out = null;
				try {
					out = new FileOutputStream(propsFile, false);
					properties.store(out, "");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					if (out != null) {
						try {
							out.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		if (hydraClient != null) {
			String databaseNames[] = HydraTest.testGetDatabases();
			for (String databaseName : databaseNames)
				HydraTest.testGetDatabase(databaseName);
			
			HydraTest.testInsert();
			
			HydraTest.testQuery();
			
			HydraTest.testUpdate();
			
			HydraTest.testDelete();
			
			HydraTest.testExecute();
		}
	}
	
	private static String testTokenAuthorization() {
		try {
			String token = testGetUnauthorizedToken();
			testAuthorizeToken(token);
			return token;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private static String testGetUnauthorizedToken() throws Exception {
		System.out.println("testGetUnauthorizedToken");
		return hydraClient.getUnauthorizedToken();
	}
	
	private static boolean testAuthorizeToken(String token) throws Exception {
		System.out.println("testAuthorizeToken");
		return hydraClient.authorizeToken(token);
	}
	
	private static String[] testGetDatabases() {
		System.out.println("");
		System.out.println("testGetDatabases");
		String[][] rows;
		String[] databases;
		try {
			rows = hydraClient.getDatabases();
			databases = new String[rows.length];
			for (int r = 0; r < databases.length; r++)
				databases[r] = rows[r][0];
		} catch (Exception e) {
			e.printStackTrace();
			databases = new String[0];
		}
		for (String database : databases)
			System.out.println("database: " + database);
		return databases;
	}
	
	private static void testGetDatabase(String database) {
		System.out.println("");
		System.out.println("testGetDatabase");
		String[][] rows;
		try {
			rows = hydraClient.getDatabase(database);
			for (String[] rowData: rows)
				for (String col : rowData)
					System.out.println(col);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testExecute() {
		System.out.println("");
		System.out.println("testExecute(myud): WHERE");
		try {
			String[][] rows = hydraClient.execute("myud", "WHERE", false);
			for (String[] rowData: rows)
				for (String col : rowData)
					System.out.println(col);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testInsert() {
		System.out.println("");
		System.out.println("testInsert(myud): VOC @ID=test, F2=testf2, F3= testf3");
		try {
			String[][] rows = hydraClient.insert("myud", "VOC", new String[]{"@ID", "F2", "F3"}, new String[]{"test", "testf2", "testf3"}, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testQuery() {
		System.out.println("");
		System.out.println("testQuery(myud): LIST VOC F2 F3 WITH @ID=\"test\"");
		try {
			String[][] rows = hydraClient.query("myud", "VOC", new String[]{"F2", "F3"}, "@ID=\"test\"", false);
			for (String[] row : rows) {
				System.out.println("F2= " + row[0]);
				System.out.println("F3= " + row[1]);
			}	
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testUpdate() {
		System.out.println("");
		System.out.println("testUpdate(myud): VOC @ID=test, F2=newf2, F3=newf3");
		try {
			String[][] rows = hydraClient.update("myud", "VOC", new String[]{"F2", "F3"}, new String[]{"newf2", "newf3"}, "@ID=\"test\"", false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void testDelete() {
		System.out.println("");
		System.out.println("testDelete(myud): VOC WITH @ID=\"test\"");
		try {
			String[][] rows = hydraClient.delete("myud", "VOC", "@ID=\"test\"", false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
