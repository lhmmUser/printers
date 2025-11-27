import './globals.css';

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {

  return (
    <>
        <div className="flex h-screen">
          <main
            className="flex-1 bg-gray-50 overflow-y-auto p-4 md:p-2 min-h-screen transition-all duration-300"
          >
            {children}
          </main>

        </div>
  
    </>
  );
}