"use client";

import { useEffect, useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Sidebar from "./components/Sidebar";


type RawOrder = {
  order_id: string;
  name?: string;
  phone_number?: string;
  city?: string;
  bookId?: string;
  bookStyle?: string;
  coverPdf?: string;
  interiorPdf?: string;
  printer?: string;
  label_url?: string;
  print_sent_at?: string;
  zip?: string;
};

type UserConfig = {
  username: string;
  password: string;
  printer: "genesis" | "yara" | "admin";
  token: string;
};

const USERS: UserConfig[] = [
  {
    username: "genesis",
    password: "G3n3s!sP@ss",
    printer: "genesis",
    token: "Q7F9sX2M8VwRkA3ZP4mJtE6D5HcLNYUeGqKBa",
  },
  {
    username: "yara",
    password: "Y@r@P#ss123",
    printer: "yara",
    token: "Zk4T8H9J2c5sA3VwP6MELXRYFQNDmUGB7qKe",
  },
  {
    username: "admin",
    password: "@dm!nAcc3ss",
    printer: "admin",
    token: "9M8qKZJ2F6XcR4A5wDHBtELPsNYGVUm",
  },
];


function formatPrintSentAt(value?: string): string {
  if (!value) return "-";

  // Remove microseconds if present (JS can't parse 6 digits properly)
  const cleaned = value.replace(/\.\d+/, "");

  // Manually split instead of letting Date() auto-convert timezone
  const [datePart, timePart] = cleaned.split("T");
  if (!datePart || !timePart) return value;

  const [year, month, day] = datePart.split("-").map(Number);
  const [hour, minute] = timePart.split(":").map(Number);

  if (
    !year || !month || !day ||
    hour === undefined || minute === undefined
  ) {
    return value;
  }

  const months = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
  ];

  const dd = day.toString().padStart(2, "0");
  const mon = months[month - 1];
  const hh = hour.toString().padStart(2, "0");
  const mm = minute.toString().padStart(2, "0");

  return `${dd} ${mon} ${hh}:${mm}`;
}

export default function GenesisShipDashboard() {
  const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "";

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(false);
  const [isShipping, setIsShipping] = useState(false);
  const [isSyncing, setIsSyncing] = useState(false);

  const [currentPage, setCurrentPage] = useState(1);
  const pageSize = 18;

  const [orders, setOrders] = useState<RawOrder[]>([]);
  const [totalOrders, setTotalOrders] = useState(0);

  const [searchInput, setSearchInput] = useState("");
  const [searchText, setSearchText] = useState("");

  const [printerKey, setPrinterKey] = useState<"genesis" | "yara">("genesis");
  const [token, setToken] = useState<string>("");
  const [role, setRole] = useState<"admin" | "printer" | null>(null);
  const [initialized, setInitialized] = useState(false);

  const [loginUsername, setLoginUsername] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [loginError, setLoginError] = useState("");
  const [isLoggingIn, setIsLoggingIn] = useState(false);

  const isMutating = isShipping || isSyncing;

  // On first load, try to read token/printer from URL (so you can share links).
  useEffect(() => {
    try {
      const params = new URLSearchParams(window.location.search);
      const p = (params.get("printer") || "genesis").toLowerCase();
      const t = params.get("token") || "";

      if (p === "yara") {
        setPrinterKey("yara");
      } else {
        setPrinterKey("genesis");
      }

      if (t === "ADMIN999") {
        setRole("admin");
      } else if (t) {
        setRole("printer");
      } else {
        setRole(null);
      }

      setToken(t);
    } catch {
      setRole(null);
      setToken("");
    } finally {
      setInitialized(true);
    }
  }, []);

  const fetchGenesisOrders = useCallback(
    async (page = 1, search = "") => {
      if (!token) {
        setOrders([]);
        setTotalOrders(0);
        return;
      }

      setLoading(true);
      try {
        const params = new URLSearchParams();
        params.set("page", String(page));
        params.set("page_size", String(pageSize));
        params.set("printer", printerKey);
        params.set("token", token);

        const trimmedSearch = search.trim();
        if (trimmedSearch) {
          params.set("search", trimmedSearch);
        }

        const res = await fetch(`${baseUrl}/orders?${params.toString()}`);
        if (!res.ok) {
          console.error("Failed to load orders", res.status, res.statusText);
          setOrders([]);
          setTotalOrders(0);
          return;
        }

        const json = await res.json();

        const data: RawOrder[] = Array.isArray(json)
          ? json
          : (json.items as RawOrder[]) ?? [];

        const total =
          typeof json.total === "number" ? json.total : data.length;

        const filtered = data.filter((o) => {
          const printer = (o.printer ?? "").toLowerCase();
          const id = (o.order_id ?? "").toUpperCase();
          return (
            printer === printerKey.toLowerCase() && !id.startsWith("TEST#")
          );
        });

        const trimmed = filtered.map((o) => ({
          order_id: o.order_id,
          name: o.name || "",
          phone_number: o.phone_number || "",
          city: o.city || "",
          bookId: o.bookId || "",
          bookStyle: o.bookStyle || "",
          coverPdf: o.coverPdf || "",
          interiorPdf: o.interiorPdf || "",
          printer: o.printer || "",
          label_url: o.label_url || "",
          print_sent_at: o.print_sent_at || "",
          zip: (o as any).zip || "",
        }));

        trimmed.sort((a, b) => {
          const parse = (v?: string) => {
            if (!v) return 0;
            let normalized = v;
            if (!/[zZ]|[+-]\d{2}:?\d{2}$/.test(v)) {
              normalized = v.replace(/(\.\d{3})\d+/, "$1") + "Z";
            }
            const d = new Date(normalized);
            return d.getTime() || 0;
          };
          return parse(b.print_sent_at) - parse(a.print_sent_at);
        });

        setOrders(trimmed);
        setTotalOrders(total);
      } catch (e) {
        console.error("fetchGenesisOrders error", e);
        setOrders([]);
        setTotalOrders(0);
      } finally {
        setLoading(false);
      }
    },
    [baseUrl, pageSize, printerKey, token]
  );

  // debounce search
  useEffect(() => {
    const handle = setTimeout(() => {
      setSearchText(searchInput.trim());
      setCurrentPage(1);
    }, 400);
    return () => clearTimeout(handle);
  }, [searchInput]);

  // refetch when page or search changes and we have a token
  useEffect(() => {
    if (!initialized || !token) return;
    fetchGenesisOrders(currentPage, searchText);
  }, [currentPage, searchText, fetchGenesisOrders, initialized, token]);

  const normalizedSearch = searchText.toLowerCase();

  const totalPages = Math.max(1, Math.ceil(totalOrders / pageSize));
  const paginatedOrders = orders;

  function getPageList(current: number, total: number, delta = 2) {
    const range: number[] = [];
    const result: (number | string)[] = [];
    const left = Math.max(1, current - delta);
    const right = Math.min(total, current + delta);

    for (let i = 1; i <= total; i++) {
      if (i === 1 || i === total || (i >= left && i <= right)) {
        range.push(i);
      }
    }

    let last = 0;
    for (const n of range) {
      if (last && n - last > 1) result.push(".");
      result.push(n);
      last = n;
    }
    return result;
  }

  const toggleSelect = (orderId: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(orderId)) next.delete(orderId);
      else next.add(orderId);
      return next;
    });
  };

  async function pollForLabels(
    orderIds: string[],
    pollIntervalMs = 2000,
    maxAttempts = 15
  ): Promise<Record<string, string | null>> {
    const result: Record<string, string | null> = {};
    for (const id of orderIds) result[id] = null;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        const params = new URLSearchParams();
        params.set("page", "1");
        params.set("page_size", "500");
        params.set("printer", printerKey);
        params.set("token", token);

        const res = await fetch(`${baseUrl}/orders?${params.toString()}`);
        if (!res.ok) {
          console.warn("pollForLabels: /orders failed", res.status);
        } else {
          const json = await res.json();
          const data: RawOrder[] = Array.isArray(json)
            ? json
            : (json.items as RawOrder[]) ?? [];

          const map = new Map<string, string | undefined>();
          for (const d of data) {
            if (d && d.order_id) {
              map.set(d.order_id, d.label_url);
            }
          }

          let allFound = true;
          for (const id of orderIds) {
            const lbl = map.get(id);
            if (lbl) {
              result[id] = lbl;
            } else {
              allFound = false;
            }
          }

          if (allFound) return result;
        }
      } catch (e) {
        console.error("pollForLabels error", e);
      }
      await new Promise((r) => setTimeout(r, pollIntervalMs));
    }

    return result;
  }

  const shipNow = async () => {
    if (selected.size === 0) {
      alert("Select at least one order.");
      return;
    }
    if (isMutating) return;

    setIsShipping(true);
    try {
      const payload = {
        order_ids: Array.from(selected),
        assign_awb: true,
        request_pickup: true,
        generate_label: true,
      };

      const res = await fetch(`${baseUrl}/shiprocket/create-from-orders`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        let msg = `HTTP ${res.status}`;
        try {
          const j = await res.json();
          msg = j.detail || JSON.stringify(j);
        } catch { }
        throw new Error(msg);
      }

      const j = await res.json();
      const createdArray: Array<{ order_id?: string }> = Array.isArray(
        j?.created
      )
        ? j.created
        : [];
      const createdIds = createdArray
        .map((c) => c.order_id)
        .filter(Boolean) as string[];

      const targetIds = createdIds.length ? createdIds : Array.from(selected);

      const pollResult = await pollForLabels(targetIds, 3000, 2);


      setOrders((prev) =>
        prev.map((o) => {
          if (pollResult[o.order_id]) {
            return { ...o, label_url: pollResult[o.order_id] || o.label_url };
          }
          return o;
        })
      );

      setSelected(new Set());
      alert(
        "Shiprocket processing done. Download links updated when labels appeared."
      );

      await fetchGenesisOrders(currentPage, searchText);
    } catch (err: any) {
      alert("Ship failed: " + err?.message);
    } finally {
      setIsShipping(false);
    }
  };

  const syncOrders = async () => {
    if (isMutating) return;

    const confirmRun = window.confirm(
      `This will try to sync labels for ALL ${printerKey.toUpperCase()} orders that already have sr_shipment_id but no label_url. Continue?`
    );
    if (!confirmRun) return;

    setIsSyncing(true);
    try {
      const res = await fetch(`${baseUrl}/shiprocket/sync-missing-labels`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(printerKey),
      });

      let j: any = {};
      try {
        j = await res.json();
      } catch (e) {
        console.warn("sync: could not parse JSON", e);
      }

      console.log("SYNC RESULT:", res.status, j);

      if (!res.ok) {
        const msg = (j && (j.detail || j.message)) || `HTTP ${res.status}`;
        throw new Error(msg);
      }

      const matched = j.matched_docs ?? 0;
      const eligible =
        j.eligible_count ?? (j.eligible_shipments?.length ?? 0);
      const succeededCount = Array.isArray(j.succeeded_shipments)
        ? j.succeeded_shipments.length
        : 0;

      await fetchGenesisOrders(currentPage, searchText);

      alert(
        `Sync completed.\n` +
        `Matched documents: ${matched}\n` +
        `Eligible shipments: ${eligible}\n` +
        `Labels written: ${succeededCount}\n` +
        `See console for full details.`
      );
    } catch (e: any) {
      alert("Sync failed: " + (e?.message ?? "Unknown error"));
      console.error("syncOrders error:", e);
    } finally {
      setIsSyncing(false);
    }
  };

  const gotoPage = (page: number) => {
    if (page < 1 || page > totalPages) return;
    setCurrentPage(page);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const handleLogin = (e: React.FormEvent) => {
    e.preventDefault();
    if (isLoggingIn) return;
    setIsLoggingIn(true);
    setLoginError("");

    const user = USERS.find(
      (u) => u.username === loginUsername && u.password === loginPassword
    );

    if (!user) {
      setLoginError("Invalid username or password.");
      setIsLoggingIn(false);
      return;
    }

    const defaultPrinter =
      user.printer === "admin" ? "genesis" : (user.printer as "genesis" | "yara");

    setToken(user.token);
    setPrinterKey(defaultPrinter);
    setRole(user.printer === "admin" ? "admin" : "printer");
    setCurrentPage(1);

    // Update URL so link is shareable
    try {
      const params = new URLSearchParams(window.location.search);
      params.set("printer", defaultPrinter);
      params.set("token", user.token);
      const newUrl = window.location.pathname + "?" + params.toString();
      window.history.replaceState(null, "", newUrl);
    } catch {
      // ignore URL errors
    }

    setIsLoggingIn(false);
  };

  const handleLogout = () => {
    setToken("");
    setRole(null);
    setSelected(new Set());
    setOrders([]);
    setTotalOrders(0);

    try {
      const params = new URLSearchParams(window.location.search);
      params.delete("token");
      const newUrl = window.location.pathname + "?" + params.toString();
      window.history.replaceState(null, "", newUrl);
    } catch {
      // ignore
    }
  };

  if (!initialized) {
    return (
      <main className="min-h-screen flex items-center justify-center">
        <div className="text-gray-500 text-sm">Loading…</div>
      </main>
    );
  }

  if (!token) {
    return (
      <motion.main
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.5 }}
        className="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-50 to-slate-100 px-4"
      >
        <motion.div
          initial={{ scale: 0.9, y: 20 }}
          animate={{ scale: 1, y: 0 }}
          transition={{ type: "spring", stiffness: 300, damping: 25 }}
          className="w-full max-w-md"
        >
          <form
            onSubmit={handleLogin}
            className="bg-white p-8 rounded-2xl shadow-xl border border-slate-200 space-y-6"
          >
            <motion.div
              initial={{ opacity: 0, y: -10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
              className="text-center"
            >
              <h1 className="text-2xl font-bold bg-gradient-to-r from-slate-800 to-slate-600 bg-clip-text text-transparent">
                DIFFRUN PRINTER
              </h1>
              <p className="text-sm text-slate-500 mt-2">Sign in to continue</p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.2 }}
              className="space-y-4"
            >
              <div className="space-y-2">
                <label className="block text-sm font-medium text-slate-700">
                  Username
                </label>
                <motion.input
                  whileFocus={{ scale: 1.02 }}
                  className="w-full border border-slate-300 rounded-xl px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200"
                  value={loginUsername}
                  onChange={(e) => setLoginUsername(e.target.value)}
                  autoComplete="username"
                />
              </div>

              <motion.div
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: 0.3 }}
                className="space-y-2"
              >
                <label className="block text-sm font-medium text-slate-700">
                  Password
                </label>
                <motion.input
                  whileFocus={{ scale: 1.02 }}
                  type="password"
                  className="w-full border border-slate-300 rounded-xl px-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200"
                  value={loginPassword}
                  onChange={(e) => setLoginPassword(e.target.value)}
                  autoComplete="current-password"
                />
              </motion.div>
            </motion.div>

            <AnimatePresence>
              {loginError && (
                <motion.div
                  initial={{ opacity: 0, height: 0 }}
                  animate={{ opacity: 1, height: "auto" }}
                  exit={{ opacity: 0, height: 0 }}
                  className="text-sm text-red-600 bg-red-50 px-3 py-2 rounded-lg border border-red-200"
                >
                  {loginError}
                </motion.div>
              )}
            </AnimatePresence>

            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              type="submit"
              disabled={isLoggingIn}
              className={`w-full py-3 rounded-xl text-sm font-semibold text-white transition-all duration-200 ${isLoggingIn
                ? "bg-blue-400 cursor-not-allowed"
                : "bg-gradient-to-r from-blue-600 to-blue-700 hover:shadow-lg"
                }`}
            >
              {isLoggingIn ? (
                <motion.span
                  animate={{ opacity: [0.5, 1, 0.5] }}
                  transition={{ duration: 1.5, repeat: Infinity }}
                  className="flex items-center justify-center gap-2"
                >
                  <motion.div
                    animate={{ rotate: 360 }}
                    transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                    className="w-4 h-4 border-2 border-white border-t-transparent rounded-full"
                  />
                  Logging in...
                </motion.span>
              ) : (
                "Login"
              )}
            </motion.button>
          </form>
        </motion.div>
      </motion.main>
    );
  }

  return (
    <motion.main
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.5 }}
      className="flex min-h-screen bg-gradient-to-br from-gray-50 to-gray-100"
    >

      <Sidebar />

      <motion.div
        initial={{ y: -20 }}
        animate={{ y: 0 }}
        className="w-full px-6 py-8"
      >

        <div>
          <div className="flex items-center justify-between gap-4">
            <motion.h2
              className="text-2xl font-bold bg-gradient-to-r from-gray-800 to-gray-600 bg-clip-text text-transparent"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.1 }}
            >
              {printerKey === "genesis" ? "Genesis" : "Yara"} x Diffrun
              Dashboard
            </motion.h2>

            <div className="flex items-center gap-2 text-xs text-gray-600">
              <span>
                Logged in as{" "}
                <span className="font-semibold">
                  {role === "admin" ? "Admin" : printerKey}
                </span>
              </span>
              <button
                onClick={handleLogout}
                className="px-3 py-1 rounded-md border border-gray-300 text-xs hover:bg-gray-100"
              >
                Logout
              </button>
            </div>
          </div>

          {role === "admin" && (
            <div className="mt-2 flex items-center gap-2 text-sm text-gray-700">
              <span>Viewing printer:</span>
              <select
                value={printerKey}
                onChange={(e) => {
                  const newPrinter = e.target.value as "genesis" | "yara";
                  setPrinterKey(newPrinter);
                  setCurrentPage(1);

                  try {
                    const params = new URLSearchParams(
                      window.location.search
                    );
                    params.set("printer", newPrinter);
                    params.set("token", token);
                    const newUrl =
                      window.location.pathname + "?" + params.toString();
                    window.history.replaceState(null, "", newUrl);
                  } catch {
                    // ignore
                  }

                  fetchGenesisOrders(1, searchText);
                }}
                className="border rounded-md px-2 py-1 bg-white"
              >
                <option value="genesis">Genesis</option>
                <option value="yara">Yara</option>
              </select>
            </div>
          )}

          <motion.div
            className="my-4 flex flex-col gap-4"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.2 }}
          >
            <div className="flex gap-3 flex-wrap">
              <motion.button
                whileHover={{
                  scale: selected.size > 0 && !isMutating ? 1.02 : 1,
                }}
                whileTap={{
                  scale: selected.size > 0 && !isMutating ? 0.98 : 1,
                }}
                onClick={shipNow}
                disabled={selected.size === 0 || isMutating}
                className={`px-6 py-3 rounded-xl text-sm font-semibold transition-all duration-200 ${selected.size === 0 || isMutating
                  ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                  : "bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-lg hover:shadow-xl"
                  }`}
              >
                {isShipping ? (
                  <motion.span
                    animate={{ opacity: [0.5, 1, 0.5] }}
                    transition={{ duration: 1.5, repeat: Infinity }}
                  >
                    Processing...
                  </motion.span>
                ) : (
                  "Ship Now"
                )}
              </motion.button>

              <motion.button
                whileHover={{ scale: !isMutating ? 1.02 : 1 }}
                whileTap={{ scale: !isMutating ? 0.98 : 1 }}
                onClick={syncOrders}
                disabled={isMutating}
                className={`px-6 py-3 rounded-xl text-sm font-semibold transition-all duration-200 ${isMutating
                  ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                  : "bg-gradient-to-r from-green-600 to-green-700 text-white shadow-lg hover:shadow-xl"
                  }`}
              >
                {isSyncing ? "Syncing…" : "Sync Orders"}
              </motion.button>

              <div className="flex-1 min-w-[200px]">
                <input
                  type="text"
                  placeholder="Search by Order ID"
                  value={searchInput}
                  onChange={(e) => setSearchInput(e.target.value)}
                  className="w-full px-4 py-3 rounded-xl border border-gray-200 bg-white text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>

            <div className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
              <div className="flex items-center justify-between px-4 py-3 border-b border-gray-100 text-xs text-gray-500">
                <div className="flex items-center gap-2">
                  <span className="inline-flex h-2 w-2 rounded-full bg-green-500"></span>
                  <span>
                    Showing {paginatedOrders.length} of {totalOrders} orders
                    {normalizedSearch && " (filtered)"}
                  </span>
                </div>
                {loading && (
                  <span className="text-blue-600 font-medium">
                    Loading…
                  </span>
                )}
              </div>

              <div className="overflow-x-auto">
                <table className="min-w-full text-sm">
                  <thead className="bg-gray-50 text-xs uppercase text-gray-500">
                    <tr>
                      <th className="px-4 py-3 text-left">
                        <input
                          type="checkbox"
                          checked={
                            paginatedOrders.length > 0 &&
                            paginatedOrders.every((o) =>
                              selected.has(o.order_id)
                            )
                          }
                          onChange={(e) => {
                            const checked = e.target.checked;
                            if (checked) {
                              const next = new Set(selected);
                              paginatedOrders.forEach((o) =>
                                next.add(o.order_id)
                              );
                              setSelected(next);
                            } else {
                              const next = new Set(selected);
                              paginatedOrders.forEach((o) =>
                                next.delete(o.order_id)
                              );
                              setSelected(next);
                            }
                          }}
                        />
                      </th>
                      <th className="px-4 py-3 text-left">Order ID</th>
                      {/* moved here */}
                      <th className="px-4 py-3 text-left">Print Sent At</th>
                      {/* then name etc. */}
                      <th className="px-4 py-3 text-left">Name</th>
                      <th className="px-4 py-3 text-left">City</th>
                      <th className="px-4 py-3 text-left">ZIP</th>
                      <th className="px-4 py-3 text-left">Phone</th>
                      <th className="px-4 py-3 text-left">Book</th>
                      <th className="px-4 py-3 text-left">Style</th>
                      <th className="px-4 py-3 text-left">Cover PDF</th>
                      <th className="px-4 py-3 text-left">
                        Interior PDF
                      </th>
                      <th className="px-4 py-3 text-left">Label</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    <AnimatePresence initial={false}>
                      {paginatedOrders.map((order) => (
                        <motion.tr
                          key={order.order_id}
                          initial={{ opacity: 0, y: 4 }}
                          animate={{ opacity: 1, y: 0 }}
                          exit={{ opacity: 0, y: -4 }}
                          transition={{ duration: 0.15 }}
                          className={`hover:bg-gray-50 ${selected.has(order.order_id)
                            ? "bg-blue-50/60"
                            : ""
                            }`}
                        >
                          <td className="px-4 py-3">
                            <input
                              type="checkbox"
                              checked={selected.has(order.order_id)}
                              onChange={() =>
                                toggleSelect(order.order_id)
                              }
                            />
                          </td>
                          <td className="px-4 py-3 text-blue-600">
                            {order.order_id}
                          </td>
                          {/* moved Print Sent At here */}
                          <td className="px-4 py-3">
                            {formatPrintSentAt(order.print_sent_at)}
                          </td>
                          <td className="px-4 py-3">{order.name}</td>
                          <td className="px-4 py-3">{order.city}</td>
                          <td className="px-4 py-3">{order.zip}</td>
                          <td className="px-4 py-3">
                            {order.phone_number}
                          </td>
                          <td className="px-4 py-3">{order.bookId}</td>
                          <td className="px-4 py-3">
                            {order.bookStyle}
                          </td>
                          <td className="px-4 py-3">
                            {order.coverPdf ? (
                              <a
                                href={order.coverPdf}
                                target="_blank"
                                rel="noreferrer"
                                className="text-blue-600 hover:underline"
                              >
                                View
                              </a>
                            ) : (
                              <span className="text-gray-400">-</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            {order.interiorPdf ? (
                              <a
                                href={order.interiorPdf}
                                target="_blank"
                                rel="noreferrer"
                                className="text-blue-600 hover:underline"
                              >
                                View
                              </a>
                            ) : (
                              <span className="text-gray-400">-</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            {order.label_url ? (
                              <a
                                href={order.label_url}
                                target="_blank"
                                rel="noreferrer"
                                download
                                className="text-blue-600 hover:underline"
                              >
                                Download
                              </a>
                            ) : (
                              <span className="text-gray-400">
                                -
                              </span>
                            )}
                          </td>
                        </motion.tr>
                      ))}
                    </AnimatePresence>

                    {paginatedOrders.length === 0 && !loading && (
                      <tr>
                        <td
                          colSpan={12}
                          className="px-4 py-8 text-center text-gray-400 text-sm"
                        >
                          No orders found.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>

              <div className="flex items-center justify-between px-4 py-3 border-t border-gray-100">
                <div className="flex items-center gap-2 text-xs">
                  {getPageList(currentPage, totalPages).map((item, idx) =>
                    item === "." ? (
                      <span key={idx} className="px-2 text-gray-400">
                        …
                      </span>
                    ) : (
                      <button
                        key={idx}
                        onClick={() => gotoPage(item as number)}
                        className={`px-2 py-1 rounded-md ${item === currentPage
                          ? "bg-blue-600 text-white"
                          : "text-gray-600 hover:bg-gray-100"
                          } text-xs`}
                      >
                        {item}
                      </button>
                    )
                  )}
                </div>

                <div className="text-sm text-gray-600 font-medium">
                  Page {currentPage} of {totalPages} — {totalOrders} orders
                  {normalizedSearch && " (filtered)"}
                </div>
              </div>
            </div>
          </motion.div>
        </div>
      </motion.div>
    </motion.main>
  );
}