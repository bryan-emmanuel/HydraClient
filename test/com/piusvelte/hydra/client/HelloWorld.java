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

public class HelloWorld {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		HydraClient hydraClient = new HydraClient("http", "localhost", 8080, "hydra", "");
		try {
			String token = hydraClient.getUnauthorizedToken();
			hydraClient.authorizeToken(token);
			hydraClient.setToken(token);
			String[][] rows = hydraClient.subroutine("demoud", "EXAMPLE", new String[]{"", "hello", "world"}, false);
			for (String[] row : rows) {
				System.out.println("arg1= " + row[0]);// always returns "RETURN"
				System.out.println("arg2= " + row[1]);// returns 2nd arg sent
				System.out.println("arg3= " + row[2]);// returns 3rd arg sent
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
