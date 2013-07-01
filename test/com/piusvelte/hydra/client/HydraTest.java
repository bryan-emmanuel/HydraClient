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

import com.piusvelte.hydra.client.HydraClient;

public class HydraTest {

	private static HydraClient hydraClient;

	// this can be run as a command line client
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		hydraClient = new HydraClient("http", "localhost", 8080, "wearebritons", "");
		String token = HydraTest.testTokenAuthorization();
		if (token == null)
			return;
		hydraClient.setToken(token);
		String databaseNames[] = HydraTest.testGetDatabases();
		for (String databaseName : databaseNames)
			HydraTest.testGetDatabase(databaseName);

		HydraTest.testInsert();

		HydraTest.testQuery();

		HydraTest.testUpdate();

		HydraTest.testQuery();

		HydraTest.testDelete();

		HydraTest.testExecute();
		
		HydraTest.testSubroutine();
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
			String[][] rows = hydraClient.insert("myud", "VOC", new String[]{"@ID", "F1", "F2"}, new String[]{"test", "firstValue", "secondValue"}, false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testQuery() {
		System.out.println("");
		System.out.println("testQuery(myud): LIST VOC F1 F2 F3 WITH @ID=\"test\"");
		try {
			String[][] rows = hydraClient.query("myud", "VOC", new String[]{"F1", "F2", "F3"}, "@ID=\"test\"", false);
			for (String[] row : rows) {
				System.out.println("F1= " + row[0]);
				System.out.println("F2= " + row[1]);
				System.out.println("F3= " + row[2]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void testUpdate() {
		System.out.println("");
		System.out.println("testUpdate(myud): VOC @ID=test, F2=newf2, F3=newf3");
		try {
			String[][] rows = hydraClient.update("myud", "VOC", new String[]{"F2", "F3"}, new String[]{"newSecondValue", "thirdValue"}, "@ID=\"test\"", false);
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
	
	private static void testSubroutine() {
		System.out.println("");
		System.out.println("testSubroutine(myud): EXAMPLE('', value1, value2)");
		try {
			String[][] rows = hydraClient.subroutine("myud", "EXAMPLE", new String[]{"arg1", "arg2", "arg3"}, false);
			for (String[] row : rows) {
				System.out.println("arg1= " + row[0]);
				System.out.println("arg2= " + row[1]);
				System.out.println("arg3= " + row[2]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
