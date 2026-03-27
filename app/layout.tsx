import type { Metadata } from "next";
import {
  Geist,
  Geist_Mono,
  Cormorant_Garamond,
  Playfair_Display,
  Noto_Serif_JP,
} from "next/font/google";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

const brandDisplay = Cormorant_Garamond({
  variable: "--font-brand-display",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  style: ["normal", "italic"],
  display: "swap",
});

const brandAccent = Playfair_Display({
  variable: "--font-brand-accent",
  subsets: ["latin"],
  weight: ["400", "500", "600", "700"],
  style: ["normal", "italic"],
  display: "swap",
});

const brandJapanese = Noto_Serif_JP({
  variable: "--font-brand-jp",
  weight: ["400", "500", "600", "700"],
  preload: false,
  display: "swap",
});

export const metadata: Metadata = {
  title: "マスタデータ",
  description: "マスタデータ",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="ja">
      <body
        className={`${geistSans.variable} ${geistMono.variable} ${brandDisplay.variable} ${brandAccent.variable} ${brandJapanese.variable} antialiased bg-transparent text-[var(--foreground)]`}
      >
        {children}
      </body>
    </html>
  );
}