/**
 * Loading spinner component.
 */

export function showLoading(container, message = "Loading...") {
  container.innerHTML = `
    <div class="loading">
      <progress></progress>
      <p>${message}</p>
    </div>
  `;
}

export function hideLoading(container) {
  const loader = container.querySelector(".loading");
  if (loader) loader.remove();
}
