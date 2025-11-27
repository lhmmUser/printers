export default function UnauthorizedPage() {
  return (
    <div className="min-h-screen flex items-center justify-center text-center">
      <h1 className="text-2xl font-bold text-red-600">
        Access denied: You're not authorized to view this dashboard.
      </h1>
    </div>
  );
}
