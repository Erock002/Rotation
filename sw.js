// Service Worker for Rotation (website + Android app)
// - App shell: network-first, falls back to the cached copy so the app opens with no connection.
// - Stats API: network-first; every good answer is saved, and when the server can't be reached
//   the last saved answer is returned with an "X-SR-Offline" header so the page can say how old it is.
// - Album art: cache-first (it never changes).
const CACHE_NAME = "rotation-v5";
const API_CACHE = "sonicreplay-api-v1";
const ART_CACHE = "sonicreplay-art-v1";
const PRECACHE = ["/", "/manifest.json", "/icon.svg", "/icon-192.png", "/icon-512.png"];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE_NAME).then((c) => c.addAll(PRECACHE)).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener("activate", (e) => {
  const keep = [CACHE_NAME, API_CACHE, ART_CACHE];
  e.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((k) => !keep.includes(k)).map((k) => caches.delete(k))))
  );
  self.clients.claim();
});

async function apiNetworkFirst(request) {
  const cache = await caches.open(API_CACHE);
  try {
    const res = await fetch(request);
    if (res && res.ok) {
      const body = await res.clone().blob();
      const headers = new Headers(res.headers);
      headers.set("X-SR-Cached-At", String(Date.now()));
      headers.delete("Content-Encoding");
      headers.delete("Content-Length");
      cache.put(request, new Response(body, { status: 200, headers }));
    }
    return res;
  } catch (err) {
    const hit = await cache.match(request);
    if (!hit) throw err;
    const headers = new Headers(hit.headers);
    headers.set("X-SR-Offline", "1");
    return new Response(await hit.blob(), { status: 200, headers });
  }
}

async function artCacheFirst(request) {
  const cache = await caches.open(ART_CACHE);
  const hit = await cache.match(request);
  if (hit) return hit;
  const res = await fetch(request);
  if (res && res.ok) cache.put(request, res.clone());
  return res;
}

self.addEventListener("fetch", (e) => {
  const req = e.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  if (url.origin !== self.location.origin) return;           // fonts etc.: let the browser handle them
  if (url.pathname === "/api/stream") return;                // audio: never cache
  if (url.pathname.startsWith("/api/auth/") || url.pathname.startsWith("/api/setup/")) return; // auth/setup: never cache

  if (url.pathname === "/api/thumb") {
    e.respondWith(artCacheFirst(req));
    return;
  }
  if (url.pathname.startsWith("/api/")) {
    e.respondWith(apiNetworkFirst(req));
    return;
  }
  e.respondWith(
    fetch(req)
      .then((res) => {
        if (res && res.status === 200) {
          const clone = res.clone();
          caches.open(CACHE_NAME).then((c) => c.put(url.pathname === "/index.html" ? "/" : req, clone));
        }
        return res;
      })
      .catch(() => caches.match(req).then((hit) => hit || caches.match("/")))
  );
});
