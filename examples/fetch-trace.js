/*
 * Reference browser helper for reporting failed fetch requests.
 *
 * The current repository exposes POST /internal/pipeline/events on the backend.
 * Set traceEndpoint to a dedicated collector endpoint when one is available.
 */

export function createFetchTrace({
  traceEndpoint = "http://localhost:8000/internal/pipeline/events",
  appName = "web-application",
  endpointId = "browser-client",
  appVersion,
  appStage,
  fetchImplementation = globalThis.fetch
} = {}) {
  if (typeof fetchImplementation !== "function") {
    throw new TypeError("fetchImplementation must be a function");
  }

  async function sendTrace(event) {
    try {
      await fetchImplementation(traceEndpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ type: event.type, payload: event }),
        keepalive: true
      });
    } catch {
      // Telemetry must never replace or mask the original application error.
    }
  }

  return async function fetchTrace(input, init = {}) {
    const request = new URL(input, globalThis.location?.origin);
    const method = (init.method || "GET").toUpperCase();
    const timestamp = Date.now();
    const resource = `${request.pathname}${request.search}`;
    const referrer = globalThis.document?.referrer || null;

    try {
      const response = await fetchImplementation(input, init);

      if (!response.ok) {
        await sendTrace({
          id: crypto.randomUUID(),
          type: "http_error",
          severity: response.status >= 500 ? "error" : "warning",
          timestamp,
          resource,
          referrer,
          app_name: appName,
          app_version: appVersion,
          app_stage: appStage,
          endpoint_id: endpointId,
          payload: {
            method,
            status: response.status,
            status_text: response.statusText
          }
        });
      }

      return response;
    } catch (error) {
      await sendTrace({
        id: crypto.randomUUID(),
        type: "network_error",
        severity: "error",
        timestamp,
        resource,
        referrer,
        app_name: appName,
        app_version: appVersion,
        app_stage: appStage,
        endpoint_id: endpointId,
        payload: {
          method,
          message: error instanceof Error ? error.message : String(error)
        }
      });

      throw error;
    }
  };
}

// Example:
// const fetchTrace = createFetchTrace({ appName: "checkout-web" });
// const response = await fetchTrace("/api/orders");
