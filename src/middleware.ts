import { clerkMiddleware, createRouteMatcher } from "@clerk/nextjs/server";
import clerkClient from "@clerk/clerk-sdk-node";
import { NextResponse } from "next/server";

// Define public routes
const isPublicRoute = createRouteMatcher(["/", "/unauthorized"]);

// Define the list of allowed email addresses
const allowedEmails = ["husain@lhmm.in", "hello@lhmm.in", "haripriya@lhmm.in", "kush@lhmm.in","arnav@lhmm.in", "fazil@lhmm.in", "chris@lhmm.in"];

export default clerkMiddleware(async (auth, req) => {
  console.log("✅ Clerk middleware hit for:", req.nextUrl.pathname);

  // Allow public access
  if (isPublicRoute(req)) return NextResponse.next();

  const { userId } = await auth();

  if (!userId) {
    console.log("⛔ Not authenticated. Redirecting to /");
    return NextResponse.redirect(new URL("/", req.url));
  }

  // ✅ Ensure the Clerk secret key is available before calling the API
  if (!process.env.CLERK_SECRET_KEY) {
    console.error("❌ CLERK_SECRET_KEY is missing from environment variables");
    return NextResponse.redirect(new URL("/unauthorized", req.url));
  }

  try {
    const user = await clerkClient.users.getUser(userId);
    const email = user.emailAddresses.find(
      (email) => email.id === user.primaryEmailAddressId
    )?.emailAddress;

    console.log("🔍 Authenticated email:", email);

    if (!email || !allowedEmails.includes(email)) {
      console.log("⛔ Unauthorized email. Redirecting to /unauthorized");
      return NextResponse.redirect(new URL("/unauthorized", req.url));
    }

    return NextResponse.next();
  } catch (err) {
    console.error("❌ Error fetching user:", err);
    return NextResponse.redirect(new URL("/unauthorized", req.url));
  }
});

export const config = {
  matcher: ["/((?!_next|.*\\..*).*)"],
};
