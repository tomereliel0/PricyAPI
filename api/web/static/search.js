const form = document.getElementById("search-form");
const statusEl = document.getElementById("status");
const resultsEl = document.getElementById("results");
const tableSortPrefs = {};
let currentItems = [];

const SORTABLE_FIELDS = {
  chain_name: { asc: "chain_asc", desc: "chain_desc", default: "chain_asc" },
  store_id: { asc: "store_id_asc", desc: "store_id_desc", default: "store_id_asc" },
  store_name: { asc: "store_name_asc", desc: "store_name_desc", default: "store_name_asc" },
  city: { asc: "city_asc", desc: "city_desc", default: "city_asc" },
  price: { asc: "price_asc", desc: "price_desc", default: "price_asc" },
  unit_price: { asc: "unit_price_asc", desc: "unit_price_desc", default: "unit_price_asc" },
  updated: { asc: "updated_asc", desc: "updated_desc", default: "updated_desc" },
};

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function compareText(a, b) {
  return String(a ?? "").localeCompare(String(b ?? ""), undefined, { sensitivity: "base" });
}

function sortPrices(prices, sortKey) {
  const list = [...prices];
  list.sort((left, right) => {
    switch (sortKey) {
      case "price_desc":
        return (toNumber(right.price) ?? -Infinity) - (toNumber(left.price) ?? -Infinity);
      case "store_id_asc":
        return compareText(left.store_id, right.store_id);
      case "store_id_desc":
        return compareText(right.store_id, left.store_id);
      case "store_name_asc":
        return compareText(left.store_name, right.store_name);
      case "store_name_desc":
        return compareText(right.store_name, left.store_name);
      case "chain_asc":
        return compareText(left.chain_name || left.chain, right.chain_name || right.chain);
      case "chain_desc":
        return compareText(right.chain_name || right.chain, left.chain_name || left.chain);
      case "city_asc":
        return compareText(left.city, right.city);
      case "city_desc":
        return compareText(right.city, left.city);
      case "unit_price_desc":
        return (toNumber(right.unit_of_measure_price) ?? -Infinity) - (toNumber(left.unit_of_measure_price) ?? -Infinity);
      case "unit_price_asc":
        return (toNumber(left.unit_of_measure_price) ?? Infinity) - (toNumber(right.unit_of_measure_price) ?? Infinity);
      case "updated_desc":
        return compareText(right.price_update_date, left.price_update_date);
      case "updated_asc":
        return compareText(left.price_update_date, right.price_update_date);
      case "price_asc":
      default:
        return (toNumber(left.price) ?? Infinity) - (toNumber(right.price) ?? Infinity);
    }
  });
  return list;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function nextSortKey(currentSortKey, field) {
  const fieldCfg = SORTABLE_FIELDS[field];
  if (!fieldCfg) {
    return currentSortKey || "price_asc";
  }
  if (currentSortKey === fieldCfg.asc) {
    return fieldCfg.desc;
  }
  if (currentSortKey === fieldCfg.desc) {
    return fieldCfg.asc;
  }
  return fieldCfg.default;
}

function sortIndicator(sortKey, field) {
  const fieldCfg = SORTABLE_FIELDS[field];
  if (!fieldCfg) {
    return "";
  }
  if (sortKey === fieldCfg.asc) {
    return " ▲";
  }
  if (sortKey === fieldCfg.desc) {
    return " ▼";
  }
  return "";
}

function renderItems(items) {
  if (!items.length) {
    resultsEl.innerHTML = '<div class="card">No results found.</div>';
    return;
  }

  const cards = items.map((item) => {
    const sortKey = tableSortPrefs[item.item_code] || "price_asc";
    const rows = sortPrices(item.prices || [], sortKey)
      .map(
        (p) => `
          <tr>
            <td>${escapeHtml(p.chain_name || p.chain)}</td>
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
          Chains: ${escapeHtml((item.chain_names || item.chains || []).join(", "))} | Barcode: ${escapeHtml(item.item_code)} |
          Min: ${escapeHtml(item.min_price)} | Max: ${escapeHtml(item.max_price)}
        </div>
        <table>
          <thead>
            <tr>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="chain_name">Chain Name${sortIndicator(sortKey, "chain_name")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="store_id">Store ID${sortIndicator(sortKey, "store_id")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="store_name">Store${sortIndicator(sortKey, "store_name")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="city">City${sortIndicator(sortKey, "city")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="price">Price${sortIndicator(sortKey, "price")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="unit_price">Unit Price${sortIndicator(sortKey, "unit_price")}</button></th>
              <th><button type="button" class="sort-header" data-item-code="${escapeHtml(item.item_code)}" data-sort-field="updated">Updated${sortIndicator(sortKey, "updated")}</button></th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </article>
    `;
  });

  resultsEl.innerHTML = cards.join("");
}

resultsEl.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLButtonElement) || !target.classList.contains("sort-header")) {
    return;
  }
  const itemCode = target.dataset.itemCode;
  const sortField = target.dataset.sortField;
  if (!itemCode || !sortField) {
    return;
  }
  const currentSortKey = tableSortPrefs[itemCode] || "price_asc";
  tableSortPrefs[itemCode] = nextSortKey(currentSortKey, sortField);
  renderItems(currentItems);
});

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
    currentItems = payload.items || [];
    renderItems(currentItems);
  } catch (error) {
    statusEl.textContent = `Error: ${error.message}`;
  }
});
