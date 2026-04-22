async function warmHealthCheck() {
    try {
        await fetch('/health');
    } catch (_error) {
        // Web scaffold only; no user-facing error yet.
    }
}

function formatCountdown(seconds) {
    const safe = Math.max(0, Math.floor(seconds));
    if (safe <= 0) return '已到期';
    const hours = Math.floor(safe / 3600);
    const minutes = Math.floor((safe % 3600) / 60);
    const secs = safe % 60;
    const parts = [];
    if (hours) parts.push(`${hours}小时`);
    if (minutes) parts.push(`${minutes}分钟`);
    if (secs || !parts.length) parts.push(`${secs}秒`);
    return parts.join('');
}

function mountCountdowns() {
    const items = Array.from(document.querySelectorAll('[data-countdown-target]'));
    if (!items.length) return;

    const tick = () => {
        const now = Date.now() / 1000;
        items.forEach((item) => {
            const target = Number(item.dataset.countdownTarget || 0);
            if (!target) {
                item.textContent = '-';
                return;
            }
            item.textContent = formatCountdown(target - now);
        });
    };

    tick();
    window.setInterval(tick, 1000);
}

warmHealthCheck();
mountCountdowns();
