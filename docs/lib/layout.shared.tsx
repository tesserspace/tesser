import Image from 'next/image';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { Github } from 'lucide-react';
import type { LinkItemType } from 'fumadocs-ui/layouts/docs';
import { SITE_NAME } from './metadata';

const links: LinkItemType[] = [
  {
    type: 'main',
    text: 'Overview',
    description: 'Understand the architecture and goals',
    url: '/docs',
  },
  {
    type: 'main',
    text: 'Getting Started',
    description: 'Install dependencies and run the CLI',
    url: '/docs/getting-started',
  },
  {
    type: 'icon',
    label: 'GitHub',
    text: 'GitHub',
    icon: <Github className="size-4" />,
    url: 'https://github.com/tesserspace/tesser',
    external: true,
  },
];

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: <NavLogo />,
      url: '/',
    },
    themeSwitch: {
      enabled: true,
      mode: 'light-dark',
    },
    githubUrl: 'https://github.com/tesserspace/tesser',
    links,
  };
}

function NavLogo() {
  return (
    <span className="inline-flex items-center gap-2 font-semibold text-fd-foreground">
      <Image
        src="/tesser-logo.png"
        alt="Tesser"
        width={28}
        height={28}
        className="rounded"
        priority
      />
      <span>{SITE_NAME}</span>
    </span>
  );
}
