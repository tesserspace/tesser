import type { Metadata } from 'next';
import { docsBaseUrl, siteUrl } from './site';

export const SITE_NAME = 'Tesser';
const TITLE = `${SITE_NAME} Documentation`;
const DESCRIPTION =
  'Tesser is a modular Rust trading framework with a CLI, strategy SDK, portfolio services, and exchange connectors.';

export const siteMetadata: Metadata = {
  metadataBase: siteUrl,
  title: {
    default: TITLE,
    template: `%s | ${SITE_NAME} Docs`,
  },
  description: DESCRIPTION,
  openGraph: {
    title: TITLE,
    description: DESCRIPTION,
    type: 'website',
    url: docsBaseUrl,
    siteName: `${SITE_NAME} Docs`,
  },
  twitter: {
    card: 'summary_large_image',
    title: TITLE,
    description: DESCRIPTION,
  },
  alternates: {
    canonical: docsBaseUrl,
  },
  robots: {
    index: true,
    follow: true,
  },
};
