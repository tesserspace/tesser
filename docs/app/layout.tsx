import { RootProvider } from 'fumadocs-ui/provider/next';
import './global.css';
import { Inter } from 'next/font/google';
import type { Viewport } from 'next';
import type { ReactNode } from 'react';
import { siteMetadata } from '@/lib/metadata';

const inter = Inter({
  subsets: ['latin'],
});

export const metadata = siteMetadata;
export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#fafafa' },
    { media: '(prefers-color-scheme: dark)', color: '#060606' },
  ],
};
export default function Layout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className={inter.className} suppressHydrationWarning>
      <body className="flex min-h-screen flex-col bg-fd-background text-fd-foreground antialiased">
        <RootProvider>{children}</RootProvider>
      </body>
    </html>
  );
}
