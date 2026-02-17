/**
 * Global filter state with event-based updates.
 * Uses DSFR select components.
 */
import { query } from "../db.js";
import { availableYears, availableDepartments, ageRange } from "../queries.js";

const state = {
  year: null, // will be set to max year on init
  department: "all",
  sex: "all",
  ageMin: null, // will be set from DB
  ageMax: null, // will be set from DB
  dbAgeMin: 0,
  dbAgeMax: 120,
  _listeners: [],
};

export function getFilters() {
  return {
    year: state.year,
    department: state.department,
    sex: state.sex,
    ageMin: state.ageMin,
    ageMax: state.ageMax,
    dbAgeMin: state.dbAgeMin,
    dbAgeMax: state.dbAgeMax,
  };
}

export function setFilter(key, value) {
  if (state[key] === value) return;
  state[key] = value;
  state._listeners.forEach((fn) => fn(getFilters()));
}

/** Update state + DOM without triggering listeners (used by router sync). */
export function setFilterSilent(key, value) {
  if (state[key] === value) return;
  state[key] = value;
  const el = document.getElementById(`filter-${key === "department" ? "dept" : key === "ageMin" ? "age-min" : key === "ageMax" ? "age-max" : key}`);
  if (el) el.value = String(value);
}

export function onFilterChange(fn) {
  state._listeners.push(fn);
}

/**
 * Render the global filter bar into the header element.
 */
export async function renderFilters(container) {
  const [years, depts, ages] = await Promise.all([
    query(availableYears()),
    query(availableDepartments()),
    query(ageRange()),
  ]);

  const dbMin = ages[0]?.min_age ?? 0;
  const dbMax = ages[0]?.max_age ?? 120;
  state.dbAgeMin = dbMin;
  state.dbAgeMax = dbMax;

  // Default to full DB range on first init
  if (state.ageMin == null) state.ageMin = dbMin;
  if (state.ageMax == null) state.ageMax = dbMax;

  // Default to latest year
  if (!state.year) {
    state.year = Math.max(...years.map((r) => r.year));
  }

  const ageOptions = Array.from({ length: dbMax - dbMin + 1 }, (_, i) => dbMin + i);

  container.innerHTML = `
    <div class="fr-select-group">
      <label class="fr-label" for="filter-year">Année</label>
      <select class="fr-select" id="filter-year">
        ${years.map((r) => `<option value="${r.year}" ${r.year === state.year ? "selected" : ""}>${r.year}</option>`).join("")}
      </select>
    </div>
    <div class="fr-select-group">
      <label class="fr-label" for="filter-dept">Département</label>
      <select class="fr-select" id="filter-dept">
        <option value="all">Tous</option>
        ${depts.map((r) => `<option value="${r.department_code}">${r.department_code}</option>`).join("")}
      </select>
    </div>
    <div class="fr-select-group">
      <label class="fr-label" for="filter-sex">Sexe</label>
      <select class="fr-select" id="filter-sex">
        <option value="all">Tous</option>
        <option value="male">Homme</option>
        <option value="female">Femme</option>
      </select>
    </div>
    <div class="fr-select-group">
      <label class="fr-label" for="filter-age-min">Âge min</label>
      <select class="fr-select" id="filter-age-min">
        ${ageOptions.map((a) => `<option value="${a}" ${a === state.ageMin ? "selected" : ""}>${a}</option>`).join("")}
      </select>
    </div>
    <div class="fr-select-group">
      <label class="fr-label" for="filter-age-max">Âge max</label>
      <select class="fr-select" id="filter-age-max">
        ${ageOptions.map((a) => `<option value="${a}" ${a === state.ageMax ? "selected" : ""}>${a}</option>`).join("")}
      </select>
    </div>
  `;

  container.querySelector("#filter-year").addEventListener("change", (e) => {
    setFilter("year", Number(e.target.value));
  });
  container.querySelector("#filter-dept").addEventListener("change", (e) => {
    setFilter("department", e.target.value);
  });
  container.querySelector("#filter-sex").addEventListener("change", (e) => {
    setFilter("sex", e.target.value);
  });
  container.querySelector("#filter-age-min").addEventListener("change", (e) => {
    const lo = Number(e.target.value);
    const hi = state.ageMax;
    setFilter("ageMin", Math.min(lo, hi));
    setFilter("ageMax", Math.max(lo, hi));
  });
  container.querySelector("#filter-age-max").addEventListener("change", (e) => {
    const hi = Number(e.target.value);
    const lo = state.ageMin;
    setFilter("ageMin", Math.min(lo, hi));
    setFilter("ageMax", Math.max(lo, hi));
  });
}
