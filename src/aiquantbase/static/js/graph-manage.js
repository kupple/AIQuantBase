document.querySelectorAll("[data-confirm]").forEach((button) => {
  button.addEventListener("click", (event) => {
    const message = button.getAttribute("data-confirm") || "确认执行这个操作吗？";
    if (!window.confirm(message)) {
      event.preventDefault();
    }
  });
});

const SCROLL_KEY = "aiquantbase-manage-scroll-y";

function persistScrollPosition() {
  window.sessionStorage.setItem(SCROLL_KEY, String(window.scrollY));
}

document.querySelectorAll("[data-preserve-scroll='true']").forEach((element) => {
  element.addEventListener("click", () => {
    persistScrollPosition();
  });
});

document.querySelectorAll("[data-href]").forEach((row) => {
  row.addEventListener("click", (event) => {
    if (event.target.closest("a, button, input, select, textarea, form")) {
      return;
    }
    persistScrollPosition();
    window.location.href = row.dataset.href;
  });

  row.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      persistScrollPosition();
      window.location.href = row.dataset.href;
    }
  });
});

document.querySelectorAll("[data-preserve-scroll='true']").forEach((link) => {
  link.addEventListener("click", () => {
    window.sessionStorage.setItem(SCROLL_KEY, String(window.scrollY));
  });
});

window.addEventListener("load", () => {
  const savedScrollY = window.sessionStorage.getItem(SCROLL_KEY);
  if (savedScrollY !== null) {
    window.scrollTo({ top: Number(savedScrollY), behavior: "auto" });
    window.sessionStorage.removeItem(SCROLL_KEY);
  }
});
