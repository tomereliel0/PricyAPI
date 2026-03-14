const form = document.getElementById("search-form");
const statusEl = document.getElementById("status");
const resultsEl = document.getElementById("results");

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderItems(items) {
  if (!items.length) {
    resultsEl.innerHTML = '<div class="card">No results found.</div>';
    return;
  }

  const cards = items.map((item) => {
    const rows = item.prices
      .map(
        (p) => `
          <tr>
            <td>${escapeHtml(p.store_id)}</td>
            <td>${escapeHtml(p.store_name)}</td>
            <td>${escapeHtml(p.city)}</td>
            <td>${escapeHtml(p.price)}</td>
            <td>${escapeHtml(p.unit_of_measure_price)}</td>
            <td>${escapeHtml(p.price_update_date)}</td>
          </tr>
        `
      )
      .join("");

    return `
      <article class="card">
        <h3>${escapeHtml(item.item_name)}</h3>
        <div class="meta">
          Chain: ${escapeHtml(item.chain)} | Barcode: ${escapeHtml(item.item_code)} |
          Min: ${escapeHtml(item.min_price)} | Max: ${escapeHtml(item.max_price)}
        </div>
        <table>
          <thead>
            <tr>
              <th>Store ID</th>
              <th>Store</th>
              <th>City</th>
              <th>Price</th>
              <th>Unit Price</th>
              <th>Updated</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </article>
    `;
  });

  resultsEl.innerHTML = cards.join("");
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const type = document.getElementById("search-type").value;
  const query = document.getElementById("query").value.trim();
  const mode = document.getElementById("mode").value;
  const limit = document.getElementById("limit").value;

  if (!query) {
    statusEl.textContent = "Please provide a query.";
    return;
  }

  statusEl.textContent = "Searching...";
  resultsEl.innerHTML = "";

  try {
    const params = new URLSearchParams();
    params.set("mode", mode);

    let endpoint = "/prices/by-name";
    if (type === "barcode") {
      endpoint = "/prices/by-barcode";
      params.set("barcode", query);
    } else {
      params.set("q", query);
      params.set("limit", limit || "50");
    }

    const response = await fetch(`${endpoint}?${params.toString()}`);
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(payload.detail || "Request failed");
    }

    statusEl.textContent = `Found ${payload.total_items} items`;
    renderItems(payload.items || []);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});
