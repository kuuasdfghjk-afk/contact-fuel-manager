/* Service Worker for Web Push & Native Notifications */
self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

// Triggered when a standard VAPID Web Push arrives from the push service
self.addEventListener("push", (event) => {
  const handlePush = async () => {
    let payload = {
      title: "🔔 期限管理アラート",
      body: "交換期限・精算期限が近づいている項目があります。",
      icon: "/icon.svg",
      badge: "/icon.svg",
      tag: "deadline-alert",
      url: "/",
    };

    if (event.data) {
      try {
        const parsed = event.data.json();
        payload = { ...payload, ...parsed };
      } catch (_) {
        const txt = event.data.text();
        if (txt) payload.body = txt;
      }
    } else {
      // Fetch the latest rich alert payload from our backend
      try {
        const res = await fetch("/api/notifications/latest-payload", { cache: "no-store" });
        if (res.ok) {
          const serverPayload = await res.json();
          payload = { ...payload, ...serverPayload };
        }
      } catch (err) {
        console.warn("Failed to fetch latest push payload:", err);
      }
    }

    return self.registration.showNotification(payload.title, {
      body: payload.body,
      icon: payload.icon || "/icon.svg",
      badge: payload.badge || "/icon.svg",
      tag: payload.tag || "deadline-alert",
      renotify: true,
      requireInteraction: true,
      data: { url: payload.url || "/" },
    });
  };

  event.waitUntil(handlePush());
});

// Also allow direct local / SSE-triggered push notification display via postMessage
self.addEventListener("message", (event) => {
  if (event.data && event.data.type === "SHOW_NOTIFICATION") {
    const p = event.data.payload || {};
    event.waitUntil(
      self.registration.showNotification(p.title || "🔔 期限管理アラート", {
        body: p.body || "",
        icon: "/icon.svg",
        badge: "/icon.svg",
        tag: p.tag || "deadline-alert-" + Date.now(),
        renotify: true,
        data: { url: p.url || "/" },
      })
    );
  }
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || "/";

  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((windowClients) => {
      for (const client of windowClients) {
        if ("focus" in client) {
          return client.focus();
        }
      }
      if (self.clients.openWindow) {
        return self.clients.openWindow(targetUrl);
      }
    })
  );
});
