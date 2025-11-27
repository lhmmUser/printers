"use client";

import { useEffect, useState, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";

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
type OrdersViewProps = {
  title?: string;
  excludeTestDiscount?: boolean;
};

function formatPrintSentAt(value?: string): string {
  if (!value) return "-";

  let normalized = value;

  if (!/[zZ]|[+-]\d{2}:?\d{2}$/.test(value)) {
    normalized = value.replace(/(\.\d{3})\d+/, "$1") + "Z";
  }

  const d = new Date(normalized);
  if (isNaN(d.getTime())) return value;

  const day = d.getDate();
  const months = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "MAY",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
  ];
  const month = months[d.getMonth()];
  const hh = d.getHours().toString().padStart(2, "0");
  const mm = d.getMinutes().toString().padStart(2, "0");

  return `${day} ${month} ${hh}:${mm}`;
}

export default function GenesisShipDashboard({
  title = "Genesis — Ship Dashboard",
  excludeTestDiscount = true,
}: OrdersViewProps) {
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

  const isMutating = isShipping || isSyncing;

  const fetchGenesisOrders = useCallback(
    async (page = 1, search = "") => {
      setLoading(true);
      try {
        const params = new URLSearchParams();
        params.set("page", String(page));
        params.set("page_size", String(pageSize));
        params.set("printer", "genesis");

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
          return printer === "genesis" && !id.startsWith("TEST#");
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
    [baseUrl]
  );

  useEffect(() => {
    const handle = setTimeout(() => {
      setSearchText(searchInput.trim());
      setCurrentPage(1);
    }, 400);
    return () => clearTimeout(handle);
  }, [searchInput]);

  useEffect(() => {
    fetchGenesisOrders(currentPage, searchText);
  }, [currentPage, searchText, fetchGenesisOrders]);

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
      if (last && n - last > 1) result.push("...");
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
        params.set("printer", "genesis");

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
        } catch {}
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

      const pollResult = await pollForLabels(targetIds, 2000, 20);

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
      "This will try to sync labels for ALL Genesis orders that already have sr_shipment_id but no label_url. Continue?"
    );
    if (!confirmRun) return;

    setIsSyncing(true);
    try {
      const res = await fetch(`${baseUrl}/shiprocket/sync-missing-labels`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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

  return (
    <motion.main 
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.5 }}
      className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100 py-3 px-6"
    >
      <motion.div
        initial={{ y: -20 }}
        animate={{ y: 0 }}
        className="max-w-9xl mx-auto"
      >
        <div className="">
          <motion.h2 
            className="text-3xl font-bold bg-gradient-to-r from-gray-800 to-gray-600 bg-clip-text text-transparent"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.1 }}
          >
            {title}
          </motion.h2>

          <motion.div 
            className="my-4 flex flex-col gap-4"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.2 }}
          >
            <div className="flex gap-3 flex-wrap">
              <motion.button
                whileHover={{ scale: selected.size > 0 && !isMutating ? 1.02 : 1 }}
                whileTap={{ scale: selected.size > 0 && !isMutating ? 0.98 : 1 }}
                onClick={shipNow}
                disabled={selected.size === 0 || isMutating}
                className={`px-6 py-3 rounded-xl text-sm font-semibold transition-all duration-200 ${
                  selected.size === 0 || isMutating
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
                className={`px-6 py-3 rounded-xl text-sm font-semibold transition-all duration-200 ${
                  isMutating
                    ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                    : "bg-gradient-to-r from-green-600 to-green-700 text-white shadow-lg hover:shadow-xl"
                }`}
              >
                {isSyncing ? (
                  <motion.span
                    animate={{ opacity: [0.5, 1, 0.5] }}
                    transition={{ duration: 1.5, repeat: Infinity }}
                  >
                    Syncing...
                  </motion.span>
                ) : (
                  "Sync Orders"
                )}
              </motion.button>
            </div>

            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.3 }}
            >
              <input
                type="text"
                placeholder="Search by Order ID (e.g. #4100)"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                className="w-full max-w-md border border-gray-200 rounded-xl px-4 py-3 text-sm bg-white shadow-sm focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-200"
              />
            </motion.div>
          </motion.div>
        </div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className="bg-white rounded-2xl shadow-sm border border-gray-200 overflow-hidden"
        >
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead className="bg-gradient-to-r from-gray-50 to-gray-75">
                <tr className="text-gray-700 font-semibold">
                  <th className="p-4"></th>
                  <th className="p-4">Order ID</th>
                  <th className="p-4">Created At</th>
                  <th className="p-4">Name</th>
                  <th className="p-4">Book Style</th>
                  <th className="p-4">Book ID</th>
                  <th className="p-4">City</th>
                  <th className="p-4">Pincode</th>
                  <th className="p-4">Phone</th>
                  <th className="p-4">Cover PDF</th>
                  <th className="p-4">Interior PDF</th>
                  <th className="p-4">Download Label</th>
                </tr>
              </thead>

              <tbody>
                {paginatedOrders.length === 0 && (
                  <tr>
                    <td colSpan={12} className="p-8 text-gray-500 text-center">
                      <motion.div
                        initial={{ opacity: 0 }}
                        animate={{ opacity: 1 }}
                        className="text-lg"
                      >
                        {loading ? "Loading..." : "No orders found."}
                      </motion.div>
                    </td>
                  </tr>
                )}

                <AnimatePresence>
                  {paginatedOrders.map((o, index) => (
                    <motion.tr
                      key={o.order_id}
                      initial={{ opacity: 0, y: 10 }}
                      animate={{ opacity: 1, y: 0 }}
                      exit={{ opacity: 0, y: -10 }}
                      transition={{ delay: index * 0.05 }}
                      className="border-t border-gray-100 hover:bg-gray-50 transition-colors duration-150"
                    >
                      <td className="p-4">
                        <motion.input
                          whileTap={{ scale: 0.9 }}
                          type="checkbox"
                          checked={selected.has(o.order_id)}
                          onChange={() => toggleSelect(o.order_id)}
                          className="w-4 h-4 text-blue-600 rounded focus:ring-blue-500"
                        />
                      </td>

                      <td className="p-4 font-medium text-blue-600">
                        {o.order_id}
                      </td>

                      <td className="p-4 text-gray-600">
                        {formatPrintSentAt(o.print_sent_at)}
                      </td>

                      <td className="p-4 font-medium text-gray-900">{o.name}</td>
                      <td className="p-4 text-gray-700">{o.bookStyle}</td>
                      <td className="p-4 text-gray-700">{o.bookId}</td>
                      <td className="p-4 text-gray-700">{o.city}</td>
                      <td className="p-4 text-gray-600">{o.zip || "-"}</td>
                      <td className="p-4 text-gray-600">{o.phone_number || "-"}</td>

                      <td className="p-4">
                        {o.coverPdf ? (
                          <motion.a
                            whileHover={{ scale: 1.05 }}
                            whileTap={{ scale: 0.95 }}
                            href={o.coverPdf}
                            target="_blank"
                            className="inline-flex items-center px-3 py-1.5 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-colors duration-200 font-medium"
                            rel="noreferrer"
                          >
                            View
                          </motion.a>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>

                      <td className="p-4">
                        {o.interiorPdf ? (
                          <motion.a
                            whileHover={{ scale: 1.05 }}
                            whileTap={{ scale: 0.95 }}
                            href={o.interiorPdf}
                            target="_blank"
                            className="inline-flex items-center px-3 py-1.5 bg-green-50 text-green-600 rounded-lg hover:bg-green-100 transition-colors duration-200 font-medium"
                            rel="noreferrer"
                          >
                            View
                          </motion.a>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>

                      <td className="p-4">
                        {o.label_url ? (
                          <motion.a
                            whileHover={{ scale: 1.05 }}
                            whileTap={{ scale: 0.95 }}
                            href={o.label_url}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center px-3 py-1.5 bg-purple-50 text-purple-600 rounded-lg hover:bg-purple-100 transition-colors duration-200 font-medium"
                          >
                            Download
                          </motion.a>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
                    </motion.tr>
                  ))}
                </AnimatePresence>
              </tbody>
            </table>
          </div>
        </motion.div>

        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.5 }}
          className="mt-6 flex items-center justify-between bg-white rounded-2xl shadow-lg p-6 border border-gray-100"
        >
          <div className="flex items-center gap-2">
            <motion.button
              whileHover={{ scale: currentPage !== 1 ? 1.05 : 1 }}
              whileTap={{ scale: currentPage !== 1 ? 0.95 : 1 }}
              onClick={() => gotoPage(currentPage - 1)}
              disabled={currentPage === 1}
              className="px-4 py-2 border border-gray-200 rounded-xl bg-white text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50 transition-all duration-200"
            >
              Prev
            </motion.button>

            {getPageList(currentPage, totalPages).map((p, idx) =>
              typeof p === "string" ? (
                <span key={`dots-${idx}`} className="px-2 text-gray-500">
                  ...
                </span>
              ) : (
                <motion.button
                  key={`page-${idx}`}
                  whileHover={{ scale: 1.05 }}
                  whileTap={{ scale: 0.95 }}
                  onClick={() => gotoPage(p as number)}
                  className={`px-4 py-2 border rounded-xl text-sm font-medium transition-all duration-200 ${
                    p === currentPage 
                      ? "bg-gradient-to-r from-blue-600 to-blue-700 text-white shadow-lg" 
                      : "bg-white border-gray-200 hover:bg-gray-50"
                  }`}
                >
                  {p}
                </motion.button>
              )
            )}

            <motion.button
              whileHover={{ scale: currentPage !== totalPages ? 1.05 : 1 }}
              whileTap={{ scale: currentPage !== totalPages ? 0.95 : 1 }}
              onClick={() => gotoPage(currentPage + 1)}
              disabled={currentPage === totalPages}
              className="px-4 py-2 border border-gray-200 rounded-xl bg-white text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50 transition-all duration-200"
            >
              Next
            </motion.button>
          </div>

          <motion.div 
            className="text-sm text-gray-600 font-medium"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.6 }}
          >
            Page {currentPage} of {totalPages} — {totalOrders} orders
            {normalizedSearch && " (filtered)"}
          </motion.div>
        </motion.div>
      </motion.div>
    </motion.main>
  );
}