/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.documentation.mock;

import java.util.Set;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ControllerServiceLookup;

public class MockControllerServiceLookup implements ControllerServiceLookup {

	@Override
	public ControllerService getControllerService(String serviceIdentifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isControllerServiceEnabled(String serviceIdentifier) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isControllerServiceEnabled(ControllerService service) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<String> getControllerServiceIdentifiers(Class<? extends ControllerService> serviceType)
			throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

}
