/*
 * Copyright 2012-2024 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi.Style;
import picocli.CommandLine.Help.ColorScheme;

public class Main {
	public static void main(String... args) {
		CommandLine cmd = new CommandLine(new AerospikeBenchmark());

		// hide the generate-completion
        CommandLine gen = cmd.getSubcommands().get("generate-completion");
        gen.getCommandSpec().usageMessage().hidden(true);

		int exitCode = cmd
			.setColorScheme(getColorScheme())
			.setParameterExceptionHandler(
                    (ex, args1) -> {
                        System.err.println("Parameter Error: " + ex.getMessage());
                        return 1;
                    })
                .setExecutionExceptionHandler(
                    (ex, commandLine, parseResult) -> {
                        System.err.println("Execution Error: " + ex.getMessage());
                        return 1;
                    })
			.execute(args);
        System.exit(exitCode);
	}

	/**
     * Creates and returns a customized color scheme for the command-line interface.
     *
     * <p>The color scheme defines how different elements of the command-line help output
     * are displayed:
     * - Commands are displayed as bold and underlined
     * - Options and parameters are displayed in yellow
     * - Option parameters are displayed in italic
     * - Error messages are displayed in bold red
     * - Stack traces are displayed in italic
     *
     * @return A configured {@link CommandLine.Help.ColorScheme} object with custom styling
     * @see CommandLine.Help#defaultColorScheme()
     */
    public static CommandLine.Help.ColorScheme getColorScheme() {
        // see also CommandLine.Help.defaultColorScheme()
        return new ColorScheme.Builder()
            .commands(Style.bold, Style.underline) // combine multiple styles
            .options(Style.fg_yellow) // yellow foreground color
            .parameters(Style.fg_yellow)
            .optionParams(Style.italic)
            .errors(Style.fg_red, Style.bold)
            .stackTraces(Style.italic)
            .build();
    }
}
