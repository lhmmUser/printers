"use client";

import React from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Home, Package, Printer, Settings } from "lucide-react";

function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="h-screen w-64 bg-gray-900 text-gray-100 flex flex-col">
      {/* Header */}
      <div className="h-16 flex items-center px-6 text-lg font-semibold border-b border-gray-800">
        Dashboard
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-4 py-6 space-y-2">

        <SidebarItem
          href="/generate_labels"
          icon={<Package size={18} />}
          label="Generate Labels"
          active={pathname === "/generate_labels"}
        />
      </nav>

    </aside>
  );
}

function SidebarItem({
  href,
  icon,
  label,
  active,
}: {
  href: string;
  icon: React.ReactNode;
  label: string;
  active: boolean;
}) {
  return (
    <Link
      href={href}
      className={`
        w-full flex items-center gap-3
        px-4 py-2 rounded-md text-sm
        transition
        ${
          active
            ? "bg-gray-800 text-white"
            : "text-gray-300 hover:bg-gray-800 hover:text-white"
        }
      `}
    >
      {icon}
      <span>{label}</span>
    </Link>
  );
}

export default Sidebar;
