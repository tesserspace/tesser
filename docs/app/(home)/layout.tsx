import { type ReactNode } from 'react';
import Link from 'next/link';
import { HomeLayout } from 'fumadocs-ui/layouts/home';
import type { LinkItemType } from 'fumadocs-ui/layouts/docs';
import { baseOptions } from '@/lib/layout.shared';
import { SITE_NAME } from '@/lib/metadata';

export default function Layout({ children }: { children: ReactNode }) {
  const options = baseOptions({ includeNavLinks: true });
  const navConfig = options.nav ?? {};

  return (
    <div className="dark min-h-screen bg-black text-white antialiased">
      <HomeLayout
        {...options}
        className="pt-0 text-white"
        nav={{
          ...navConfig,
          transparentMode: navConfig.transparentMode ?? 'always',
          component: (
            <OverlayNavigation
              title={navConfig.title}
              url={navConfig.url}
              links={options.links}
              githubUrl={options.githubUrl}
              siteLabel={SITE_NAME}
            />
          ),
        }}
      >
        {children}
      </HomeLayout>
    </div>
  );
}

function OverlayNavigation({
  title,
  url = '/',
  links = [],
  githubUrl,
  siteLabel,
}: {
  title?: ReactNode;
  url?: string;
  links?: LinkItemType[];
  githubUrl?: string;
  siteLabel: string;
}) {
  const navLinks = links.filter(isPrimaryNavLink);

  const brandLabel = typeof title === 'string' ? title : siteLabel;
  const ariaLabel =
    typeof title === 'string' ? title : `${siteLabel} Home`.trim();

  return (
    <nav className="fixed top-0 left-0 w-full z-50 flex justify-between items-center px-6 py-6 md:px-12 pointer-events-none mix-blend-difference text-white">
      <Link
        href={url}
        className="pointer-events-auto flex items-center gap-2"
        aria-label={ariaLabel}
      >
        <div className="w-8 h-8 bg-white rounded-lg flex items-center justify-center">
          <div className="w-4 h-4 bg-black rounded-sm rotate-45" />
        </div>
        <span className="font-bold text-xl tracking-tight">{brandLabel}</span>
      </Link>
      <div className="pointer-events-auto hidden md:flex gap-8 text-sm font-medium">
        {navLinks.map((link) => (
          <Link
            key={link.url}
            href={link.url}
            className="transition-opacity hover:opacity-70"
          >
            {link.text}
          </Link>
        ))}
        {githubUrl && (
          <a
            href={githubUrl}
            target="_blank"
            rel="noreferrer"
            className="transition-opacity hover:opacity-70"
          >
            GitHub
          </a>
        )}
      </div>
      <div className="pointer-events-auto">
        <Link
          href="/docs"
          className="hidden md:block px-5 py-2 text-sm font-bold border border-white/20 rounded-full hover:bg-white hover:text-black transition-all"
        >
          Launch App
        </Link>
      </div>
    </nav>
  );
}

function isPrimaryNavLink(
  link: LinkItemType,
): link is Extract<LinkItemType, { url: string; text: ReactNode }> {
  const displayTarget = link.on ?? 'all';

  if (!['nav', 'all'].includes(displayTarget)) return false;
  if (link.type === 'icon' || link.type === 'menu' || link.type === 'custom')
    return false;

  return true;
}
