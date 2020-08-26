package org.corfudb.common.metrics.servers;

import io.prometheus.client.exporter.MetricsServlet;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Slf4j
public class PrometheusMetricsServerlet extends MetricsServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        log.info("Doing get");
        super.doGet(req, resp);
        log.info("Get Done");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        log.info("Doing Post");
        this.doGet(req, resp);
        log.info("Post Done");
    }
}
