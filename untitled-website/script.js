const page = window.location.pathname.split("/").pop() || "index.html";
const loggedIn = sessionStorage.getItem("untitled_logged_in") === "true";
const nav = document.getElementById("navbar");
const footer = document.getElementById("footer");
const escapeHtml = value => String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
const getUser = () => {
    try {
        return JSON.parse(sessionStorage.getItem("untitled_user") || "null");
    } catch {
        return null;
    }
};
const user = getUser();
const avatar = user?.id && user?.avatar
    ? `https://cdn.discordapp.com/avatars/${encodeURIComponent(user.id)}/${encodeURIComponent(user.avatar)}.png?size=128`
    : "https://cdn.discordapp.com/embed/avatars/0.png";
const username = user?.globalName || user?.username || "Account";
const tag = user?.username ? `@${user.username}` : "";
const setLoginState = value => {
    sessionStorage.setItem("untitled_logged_in", value ? "true" : "false");
};
const requireLogin = () => {
    if (page === "login.html") {
        return false;
    }
    if (sessionStorage.getItem("untitled_logged_in") !== "true") {
        window.location.replace("login.html");
        return true;
    }
    return false;
};
const getPath = file => {
    if (file === "index.html") {
        return page === "index.html" || page === "";
    }
    return page === file;
};
const link = (file, text) => {
    return `<a href="${file}"${getPath(file) ? ' class="active"' : ""}>${text}</a>`;
};
const buildNav = () => {
    if (!nav || page === "login.html") {
        return;
    }
    nav.innerHTML = `
        <nav>
            <a href="index.html" class="logo">untitled</a>
            <button class="nav-toggle" type="button" aria-label="Open navigation" aria-expanded="false">
                <span></span>
                <span></span>
                <span></span>
            </button>
            <div class="nav-links">
                ${link("features.html", "Features")}
                ${link("prices.html", "Pricing")}
                ${link("information.html", "Information")}
                ${link("docs.html", "Docs")}
                ${link("api.html", "API")}
            </div>
            <div class="account">
                <button class="account-button" type="button" aria-expanded="false">
                    <img src="${escapeHtml(avatar)}" alt="${escapeHtml(username)}">
                    <span class="account-details">
                        <strong>${escapeHtml(username)}</strong>
                        ${tag ? `<small>${escapeHtml(tag)}</small>` : ""}
                    </span>
                    <span class="account-arrow">⌄</span>
                </button>
                <div class="account-menu">
                    <div class="account-menu-user">
                        <img src="${escapeHtml(avatar)}" alt="${escapeHtml(username)}">
                        <div>
                            <strong>${escapeHtml(username)}</strong>
                            ${tag ? `<span>${escapeHtml(tag)}</span>` : ""}
                        </div>
                    </div>
                    <div class="account-menu-line"></div>
                    <a href="account.html">Account</a>
                    <a href="settings.html">Settings</a>
                    <button type="button" data-logout>Logout</button>
                </div>
            </div>
        </nav>
    `;
};
const buildFooter = () => {
    if (!footer) {
        return;
    }
    footer.innerHTML = `
        <div class="footer-inner">
            <span class="footer-logo">untitled</span>
            <span>Built for the VRFS community.</span>
        </div>
    `;
};
const setupNav = () => {
    if (!nav || page === "login.html") {
        return;
    }
    const button = nav.querySelector(".nav-toggle");
    const links = nav.querySelector(".nav-links");
    if (button && links) {
        button.addEventListener("click", () => {
            const open = links.classList.toggle("open");
            button.setAttribute("aria-expanded", String(open));
            button.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");
        });
        links.querySelectorAll("a").forEach(item => {
            item.addEventListener("click", () => {
                links.classList.remove("open");
                button.setAttribute("aria-expanded", "false");
                button.setAttribute("aria-label", "Open navigation");
            });
        });
    }
    const accountButton = nav.querySelector(".account-button");
    const accountMenu = nav.querySelector(".account-menu");
    if (accountButton && accountMenu) {
        accountButton.addEventListener("click", event => {
            event.stopPropagation();
            const open = accountMenu.classList.toggle("open");
            accountButton.setAttribute("aria-expanded", String(open));
        });
        accountMenu.addEventListener("click", event => {
            event.stopPropagation();
        });
        document.addEventListener("click", () => {
            accountMenu.classList.remove("open");
            accountButton.setAttribute("aria-expanded", "false");
        });
    }
};
const setupAccount = () => {
    if (!loggedIn || !nav) {
        return;
    }
    const button = nav.querySelector(".account-button");
    const menu = nav.querySelector(".account-menu");
    if (!button || !menu) {
        return;
    }
    button.addEventListener("click", event => {
        event.stopPropagation();
        const open = menu.classList.toggle("open");
        button.setAttribute("aria-expanded", String(open));
    });
};
const setupLogin = () => {
    if (page !== "login.html") {
        return;
    }
    const button = document.querySelector(".discord-login");
    if (!button) {
        return;
    }
    button.addEventListener("click", () => {
        window.location.href = "/api/auth/discord";
    });
};
const setupLogout = () => {
    document.querySelectorAll("[data-logout]").forEach(button => {
        button.addEventListener("click", async () => {
            button.disabled = true;
            try {
                await fetch("/api/auth/logout", {
                    method: "POST",
                    credentials: "include",
                    headers: {
                        Accept: "application/json"
                    }
                });
            } catch {
            }
            setLoginState(false);
            sessionStorage.removeItem("untitled_user");
            window.location.replace("login.html");
        });
    });
};
const loadUser = async () => {
    if (!loggedIn || page === "login.html") {
        return;
    }
    try {
        const response = await fetch("/api/auth/me", {
            method: "GET",
            credentials: "include",
            headers: {
                Accept: "application/json"
            }
        });
        if (response.status === 401 || response.status === 403) {
            setLoginState(false);
            sessionStorage.removeItem("untitled_user");
            window.location.replace("login.html");
            return;
        }
        if (!response.ok) {
            return;
        }
        const data = await response.json();
        if (!data?.user) {
            return;
        }
        setLoginState(true);
        sessionStorage.setItem("untitled_user", JSON.stringify(data.user));
    } catch {
        return;
    }
};
const setupProtectedUi = () => {
    if (!loggedIn) {
        return;
    }
    document.querySelectorAll("[data-user-name]").forEach(item => {
        item.textContent = username;
    });
    document.querySelectorAll("[data-user-avatar]").forEach(item => {
        item.src = avatar;
        item.alt = `${username} avatar`;
    });
    document.querySelectorAll("[data-user-id]").forEach(item => {
        if (user?.id) {
            item.textContent = user.id;
        }
    });
    document.querySelectorAll("[data-user-tag]").forEach(item => {
        item.textContent = tag;
    });
};
const setupLoginNotice = () => {
    const notice = document.querySelector("[data-login-required]");
    if (!notice || loggedIn) {
        return;
    }
    notice.hidden = false;
};
const closeOnEscape = event => {
    if (event.key !== "Escape") {
        return;
    }
    const links = nav?.querySelector(".nav-links");
    const button = nav?.querySelector(".nav-toggle");
    const menu = nav?.querySelector(".account-menu");
    const accountButton = nav?.querySelector(".account-button");
    if (links?.classList.contains("open")) {
        links.classList.remove("open");
        button?.setAttribute("aria-expanded", "false");
        button?.setAttribute("aria-label", "Open navigation");
    }
    if (menu?.classList.contains("open")) {
        menu.classList.remove("open");
        accountButton?.setAttribute("aria-expanded", "false");
    }
};
const setupExternalLinks = () => {
    document.querySelectorAll('a[href^="http://"], a[href^="https://"]').forEach(item => {
        const url = new URL(item.href, window.location.href);
        if (url.origin !== window.location.origin) {
            item.target = "_blank";
            item.rel = "noopener noreferrer";
        }
    });
};
const setupButtons = () => {
    document.querySelectorAll("[data-action]").forEach(button => {
        const action = button.dataset.action;
        if (!action) {
            return;
        }
        if (action === "login") {
            button.addEventListener("click", () => {
                window.location.replace("login.html");
            });
        }
        if (action === "dashboard") {
            button.addEventListener("click", () => {
                if (sessionStorage.getItem("untitled_logged_in") === "true") {
                    window.location.href = "dashboard.html";
                } else {
                    window.location.replace("login.html");
                }
            });
        }
        if (action === "premium") {
            button.addEventListener("click", () => {
                window.location.href = "prices.html";
            });
        }
    });
};
const setActiveLinks = () => {
    nav?.querySelectorAll(".nav-links a").forEach(item => {
        const href = item.getAttribute("href");
        if (!href) {
            return;
        }
        const file = href.split("/").pop() || "index.html";
        if (file === page) {
            item.classList.add("active");
        }
    });
};
const checkPage = () => {
    if (page !== "login.html") {
        document.documentElement.classList.add("logged-page");
    }
};
const start = async () => {
    if (requireLogin()) {
        return;
    }
    buildNav();
    buildFooter();
    setupNav();
    setupAccount();
    setupLogin();
    setupLogout();
    setupLoginNotice();
    setupProtectedUi();
    setupExternalLinks();
    setupButtons();
    setActiveLinks();
    checkPage();
    await loadUser();
};
document.addEventListener("keydown", closeOnEscape);
document.addEventListener("DOMContentLoaded", start);