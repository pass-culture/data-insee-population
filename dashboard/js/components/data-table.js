/**
 * Reusable sortable table with CSV export.
 */

/**
 * Render a sortable data table.
 * @param {HTMLElement} container
 * @param {Array<Object>} data - array of row objects
 * @param {Array<{key: string, label: string, format?: Function, numeric?: boolean}>} columns
 */
export function renderDataTable(container, data, columns) {
  let sortKey = null;
  let sortDir = "asc";

  function render() {
    const sorted = [...data];
    if (sortKey) {
      sorted.sort((a, b) => {
        const va = a[sortKey];
        const vb = b[sortKey];
        if (va == null) return 1;
        if (vb == null) return -1;
        const cmp = typeof va === "number" ? va - vb : String(va).localeCompare(String(vb));
        return sortDir === "asc" ? cmp : -cmp;
      });
    }

    const html = `
      <div style="display:flex;justify-content:flex-end;margin-bottom:0.5rem;">
        <button class="export-btn" data-action="csv">Export CSV</button>
      </div>
      <div class="data-table-wrapper">
        <table class="data-table">
          <thead>
            <tr>
              ${columns
                .map(
                  (col) =>
                    `<th data-key="${col.key}" class="${sortKey === col.key ? (sortDir === "asc" ? "sort-asc" : "sort-desc") : ""}">${col.label}</th>`
                )
                .join("")}
            </tr>
          </thead>
          <tbody>
            ${sorted
              .map(
                (row) =>
                  `<tr>${columns
                    .map((col) => {
                      const val = row[col.key];
                      const formatted = col.format ? col.format(val) : val;
                      return `<td class="${col.numeric ? "num" : ""}">${formatted ?? ""}</td>`;
                    })
                    .join("")}</tr>`
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;

    container.innerHTML = html;

    // Sort handlers
    container.querySelectorAll("th[data-key]").forEach((th) => {
      th.addEventListener("click", () => {
        const key = th.dataset.key;
        if (sortKey === key) {
          sortDir = sortDir === "asc" ? "desc" : "asc";
        } else {
          sortKey = key;
          sortDir = "asc";
        }
        render();
      });
    });

    // CSV export
    container.querySelector('[data-action="csv"]')?.addEventListener("click", () => {
      exportCSV(data, columns);
    });
  }

  render();
}

function exportCSV(data, columns) {
  const header = columns.map((c) => c.label).join(",");
  const rows = data.map((row) =>
    columns.map((c) => {
      const v = row[c.key];
      if (v == null) return "";
      if (typeof v === "string" && v.includes(",")) return `"${v}"`;
      return v;
    }).join(",")
  );
  const csv = [header, ...rows].join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "export.csv";
  a.click();
  URL.revokeObjectURL(url);
}
