/**
 * Entry point: DuckDB init, hash-based router, DSFR navigation.
 */
import "@gouvfr/dsfr/dist/dsfr.min.css";
import "@gouvfr/dsfr/dist/dsfr.module.min.js";
import "@gouvfr/dsfr-chart";
import "@gouvfr/dsfr-chart/css";

import { initDB } from "./db.js";
import { renderFilters, onFilterChange, setFilterSilent } from "./components/filters.js";
import { renderOverview } from "./pages/overview.js";
import { renderPyramid } from "./pages/pyramid.js";
import { renderGeography } from "./pages/geography.js";
import { renderTemporal } from "./pages/temporal.js";
import { renderDomTom } from "./pages/dom-tom.js";
import { renderPrecision } from "./pages/precision.js";
import { renderDepartmentDetail } from "./pages/department-detail.js";

const pages = {
  overview: renderOverview,
  pyramid: renderPyramid,
  geography: renderGeography,
  temporal: renderTemporal,
  "dom-tom": renderDomTom,
  precision: renderPrecision,
};

let currentPage = null;

function parseHash() {
  const hash = location.hash.slice(1) || "overview";
  // Handle department detail routes like "department/75"
  if (hash.startsWith("department/")) {
    const code = hash.split("/")[1];
    return { page: "department", params: { code } };
  }
  return { page: hash, params: {} };
}

function updateNav(page) {
  document.querySelectorAll(".fr-nav__link[data-page]").forEach((a) => {
    if (a.dataset.page === page) {
      a.setAttribute("aria-current", "page");
    } else {
      a.removeAttribute("aria-current");
    }
  });
}

async function navigate() {
  const { page, params } = parseHash();
  const content = document.getElementById("page-content");

  if (currentPage === location.hash) return;
  currentPage = location.hash;

  updateNav(page);
  content.innerHTML = "";

  // Hide filters on pages that don't use them
  const filtersEl = document.getElementById("global-filters");
  if (filtersEl) {
    filtersEl.style.display = page === "precision" ? "none" : "";
  }

  if (page === "department") {
    setFilterSilent("department", params.code);
    await renderDepartmentDetail(content, params.code);
  } else if (pages[page]) {
    await pages[page](content);
  } else {
    content.innerHTML = `<div class="fr-alert fr-alert--error"><p class="fr-alert__title">Page introuvable : ${page}</p></div>`;
  }
}

async function init() {
  try {
    await initDB();

    // Remove init loading
    const initLoading = document.getElementById("loading-init");
    if (initLoading) initLoading.remove();

    // Render global filters
    await renderFilters(document.getElementById("global-filters"));

    // On filter change, re-render current page
    onFilterChange(() => {
      currentPage = null; // force re-render
      navigate();
    });

    // Initial navigation
    await navigate();

    // Hash change listener
    window.addEventListener("hashchange", navigate);
  } catch (err) {
    console.error("Initialization failed:", err);
    document.getElementById("page-content").innerHTML = `
      <div class="fr-alert fr-alert--error">
        <h3 class="fr-alert__title">Erreur d'initialisation</h3>
        <p>${err.message}</p>
        <p>Vérifiez que vous avez exécuté <code>make dashboard-prepare</code> pour générer les fichiers de données.</p>
      </div>
    `;
  }
}

init();
