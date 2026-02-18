/**
 * Loading spinner component.
 */

export function showLoading(container, message = "Chargement\u2026") {
  container.innerHTML = `
    <div class="dashboard-loading-init">
      <div class="fr-container">
        <p class="fr-h4">${message}</p>
        <progress class="fr-progress" style="width:300px;max-width:100%"></progress>
      </div>
    </div>
  `;
}

export function hideLoading(container) {
  const loader = container.querySelector(".loading");
  if (loader) loader.remove();
}
