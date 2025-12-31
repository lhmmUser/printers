"use client";

import { useEffect, useRef, useState } from "react";

export default function ScanPage() {
  const inputRef = useRef<HTMLInputElement>(null);

  const [orderId, setOrderId] = useState("");
  const [processedOrderId, setProcessedOrderId] = useState<string | null>(null);
  const [status, setStatus] = useState<string>("");
  const [labelUrl, setLabelUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  async function generateLabel(id: string) {
    if (!id) return;

    setLoading(true);
    setProcessedOrderId(null);
    setStatus(`Processing order ${id}…`);
    setLabelUrl(null);

    try {
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/scan-order`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ order_id: id }),
        }
      );

      const data = await res.json();

      if (data.label_url) {
        setProcessedOrderId(id);
        setStatus(`✅ Shipping label for order ID "${id}" generated successfully`);
        setLabelUrl(data.label_url);

        // Auto-open PDF in new tab
        window.open(data.label_url, "_blank");
      } else {
        setStatus(`⚠️ Unable to generate label for order ID "${id}"`);
      }
    } catch {
      setStatus("❌ Something went wrong. Please try again.");
    } finally {
      setLoading(false);
      setOrderId("");
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter") {
      generateLabel(orderId.trim());
    }
  }

  function handleGenerateClick() {
    generateLabel(orderId.trim());
  }

  function handleDownload() {
    if (!labelUrl) return;
    window.open(labelUrl, "_blank");
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        background: `
          radial-gradient(circle at top left, rgba(59,130,246,0.08), transparent 60%),
          radial-gradient(circle at bottom right, rgba(34,197,94,0.08), transparent 60%),
          linear-gradient(180deg, #f9fafb 0%, #f3f4f6 100%)
        `,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 24,
      }}
    >
      <div
        style={{
          width: 480,
          background: "#ffffff",
          borderRadius: 16,
          padding: "32px",
          boxShadow:
            "0 20px 40px rgba(0,0,0,0.08), 0 4px 10px rgba(0,0,0,0.04)",
        }}
      >
        {/* Header */}
        <div style={{ textAlign: "center", marginBottom: 28 }}>
          <h1
            style={{
              fontSize: 26,
              fontWeight: 700,
              marginBottom: 6,
              color: "#111827",
            }}
          >
            Shipping Label Generator
          </h1>
          <p style={{ fontSize: 14, color: "#6b7280" }}>
            Scan the barcode or enter the order ID to generate a shipping label
          </p>
        </div>

        {/* Input */}
        <label
          style={{
            fontSize: 13,
            fontWeight: 500,
            color: "#374151",
            marginBottom: 6,
            display: "block",
          }}
        >
          Order ID
        </label>

        <input
          ref={inputRef}
          value={orderId}
          onChange={(e) => setOrderId(e.target.value)}
          onKeyDown={handleKeyDown}
          disabled={loading}
          placeholder="Scan barcode or type order ID"
          style={{
            width: "100%",
            fontSize: 20,
            padding: "14px 16px",
            borderRadius: 10,
            border: "1.5px solid #e5e7eb",
            outline: "none",
            marginBottom: 12,
          }}
        />

        {/* Generate Button */}
        <button
          onClick={handleGenerateClick}
          disabled={loading || !orderId.trim()}
          style={{
            width: "100%",
            padding: "14px 16px",
            fontSize: 16,
            fontWeight: 600,
            borderRadius: 10,
            border: "none",
            cursor: loading ? "not-allowed" : "pointer",
            background: loading
              ? "#9ca3af"
              : "linear-gradient(90deg, #16a34a, #22c55e)",
            color: "#ffffff",
            marginTop: 8,
            marginBottom: 18,
            boxShadow: loading
              ? "none"
              : "0 6px 16px rgba(34,197,94,0.35)",
          }}
        >
          {loading ? "Generating Label…" : "Generate Label"}
        </button>

        {/* Status */}
        <div
          style={{
            minHeight: 44,
            fontSize: 14,
            color: "#374151",
            marginBottom: 14,
            textAlign: "center",
          }}
        >
          {status}
        </div>

        {/* Download Button */}
        {labelUrl && processedOrderId && (
          <button
            onClick={handleDownload}
            style={{
              width: "100%",
              padding: "14px 16px",
              fontSize: 16,
              fontWeight: 500,
              borderRadius: 10,
              border: "none",
              cursor: "pointer",
              background: "linear-gradient(90deg, #2563eb, #3b82f6)",
              color: "#ffffff",
              boxShadow: "0 6px 16px rgba(59,130,246,0.35)",
            }}
          >
            ⬇️ Download Shipping Label (PDF)
          </button>
        )}
      </div>
    </div>
  );
}
