import Image from 'next/image';
import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { SITE_NAME } from './metadata';

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
