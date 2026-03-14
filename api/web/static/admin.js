const statusEl = document.getElementById("admin-status");
const outputEl = document.getElementById("admin-output");
let workerPollTimer = null;

function tokenHeaders() {
  const token = document.getElementById("token").value.trim();
  return token ? { "X-Admin-Token": token } : {};
}

function show(data) {
  outputEl.textContent = typeof data === "string" ? data : JSON.stringify(data, null, 2);
}

async function callApi(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      ...(options.headers || {}),
      ...tokenHeaders(),
    },
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.detail || JSON.stringify(payload));
  }
  return payload;
}

function stopWorkerPolling() {
  if (workerPollTimer) {
    clearInterval(workerPollTimer);
    workerPollTimer = null;
  }
}

function startWorkerPolling() {
  stopWorkerPolling();
  workerPollTimer = setInterval(async () => {
    try {
      const payload = await callApi("/admin/pipeline/all/status");
      show(payload);
      const worker = payload.worker || {};
      if (!worker.running) {
        statusEl.textContent = "All-pipelines worker finished";
        stopWorkerPolling();
      } else {
        const done = worker.completed_chains || 0;
        const total = worker.total_chains || 0;
        const current = worker.current_chain || "-";
        statusEl.textContent = `All-pipelines worker running (${done}/${total}), current: ${current}`;
      }
    } catch (error) {
      stopWorkerPolling();
      statusEl.textContent = `Status polling error: ${error.message}`;
    }
  }, 5000);
}

document.getElementById("btn-meta").addEventListener("click", async () => {
  statusEl.textContent = "Loading metadata...";
  try {
    const payload = await callApi("/meta");
    statusEl.textContent = "Metadata loaded";
    show(payload);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});

document.getElementById("btn-reload").addEventListener("click", async () => {
  const mode = document.getElementById("mode").value;
  statusEl.textContent = "Reloading index...";
  try {
    const payload = await callApi(`/admin/reload?mode=${encodeURIComponent(mode)}`, { method: "POST" });
    statusEl.textContent = "Reload complete";
    show(payload);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});

document.getElementById("btn-pipeline").addEventListener("click", async () => {
  const chain = document.getElementById("chain").value;
  const mode = document.getElementById("mode").value;
  const maxBranches = document.getElementById("max-branches").value;
  const maxWorkers = document.getElementById("max-workers").value;
  const insecure = document.getElementById("insecure").value;

  const params = new URLSearchParams();
  params.set("chain", chain);
  params.set("mode", mode);
  params.set("max_branches", maxBranches || "0");
  params.set("max_workers", maxWorkers || "6");
  params.set("insecure", insecure || "false");

  statusEl.textContent = "Running pipeline...";
  try {
    const payload = await callApi(`/admin/pipeline?${params.toString()}`, { method: "POST" });
    statusEl.textContent = "Pipeline completed";
    show(payload);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});

document.getElementById("btn-pipeline-all").addEventListener("click", async () => {
  const mode = document.getElementById("mode").value;
  const maxBranches = document.getElementById("max-branches").value;
  const maxWorkers = document.getElementById("max-workers").value;
  const insecure = document.getElementById("insecure").value;
  const reloadAfterAll = document.getElementById("reload-after-all").value;

  const params = new URLSearchParams();
  params.set("mode", mode);
  params.set("max_branches", maxBranches || "0");
  params.set("max_workers", maxWorkers || "6");
  params.set("insecure", insecure || "false");
  params.set("reload_after", reloadAfterAll || "true");

  statusEl.textContent = "Starting all-pipelines worker...";
  try {
    const payload = await callApi(`/admin/pipeline/all?${params.toString()}`, { method: "POST" });
    statusEl.textContent = "All-pipelines worker started";
    show(payload);
    startWorkerPolling();
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});

document.getElementById("btn-pipeline-all-status").addEventListener("click", async () => {
  statusEl.textContent = "Loading all-pipelines worker status...";
  try {
    const payload = await callApi("/admin/pipeline/all/status");
    const worker = payload.worker || {};
    if (worker.running) {
      statusEl.textContent = "All-pipelines worker is running";
      startWorkerPolling();
    } else {
      statusEl.textContent = "All-pipelines worker is idle";
      stopWorkerPolling();
    }
    show(payload);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});
