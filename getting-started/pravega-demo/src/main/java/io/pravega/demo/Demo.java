/*
 * Copyright (c) Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.demo;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.net.URI;

import static io.pravega.demo.DemoUtils.help;

public class Demo {

	public static void main(String[] args) {
		CommandLine cli = null;

		Options options = new Options();
		options.addOption("controller", true,"pravega controller ip:port");
		options.addOption("sr", true,"schema registry ip:port");
		options.addOption("scope", true, "pravega scope");

		CommandLineParser parser = new DefaultParser();
		try {
			cli = parser.parse(options, args);
		}
		catch (ParseException e) {
			help(options);
			System.exit(1);
		}

		if (!cli.hasOption("controller") ||
				!cli.hasOption("sr") ||
				!cli.hasOption("scope")) {
			help(options);
			System.exit(1);
		}

		try {
			new DemoRunner(URI.create("tcp://" + cli.getOptionValue("controller")),
					URI.create("http://" + cli.getOptionValue("sr")),
					cli.getOptionValue("scope"))
					.run();
		}
		catch (IOException e) {
			System.out.println(e);
			System.exit(1);
		}
	}
}