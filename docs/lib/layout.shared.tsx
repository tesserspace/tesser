import Image from 'next/image';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import type { LinkItemType } from 'fumadocs-ui/layouts/docs';
import { SITE_NAME } from './metadata';

const homeLinks: LinkItemType[] = [
  {
    type: 'main',
    text: 'Docs',
    description: 'Explore guides and references',
    url: '/docs',
    on: 'nav',
    active: 'nested-url',
  },
  {
    type: 'main',
    text: 'Playground',
    description: 'Run a JS backtest in the browser',
    url: '/playground/backtest',
    on: 'nav',
    active: 'nested-url',
  },
  {
    type: 'main',
    text: 'Blog',
    description: 'Hear from the Tesser team and community',
    url: '/blog',
    on: 'nav',
    active: 'nested-url',
  },
];

type BaseOptionsConfig = {
  includeNavLinks?: boolean;
};

export function baseOptions(
  config: BaseOptionsConfig = {},
): BaseLayoutProps {
  const { includeNavLinks = false } = config;

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
    links: includeNavLinks ? homeLinks : undefined,
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
