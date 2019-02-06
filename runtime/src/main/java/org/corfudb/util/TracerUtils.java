package org.corfudb.util;

import io.jaegertracing.Configuration;
import io.jaegertracing.Configuration.ReporterConfiguration;
import io.jaegertracing.Configuration.SamplerConfiguration;
import io.jaegertracing.internal.JaegerTracer;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TracerUtils {
	private TracerUtils() {
		// Preventing instantiation of this utility class
	}

	/**
	 * The factory method to generate a JaegerTracer instance
	 * @param service
	 * @return
	 */
	public static JaegerTracer initTracer(String service) {
		SamplerConfiguration samplerConfig = SamplerConfiguration.fromEnv().withType("const").withParam(1);
		ReporterConfiguration reporterConfig = ReporterConfiguration.fromEnv().withLogSpans(true);
		Configuration config = new Configuration(service).withSampler(samplerConfig).withReporter(reporterConfig);
		return config.getTracer();
	}
}
