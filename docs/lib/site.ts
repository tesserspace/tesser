const PROD_URL = 'https://tesser.space';
const LOCAL_URL = 'http://localhost:3000';
const DOCS_PATH = '/docs';

function resolveSiteUrl() {
  if (process.env.NEXT_PUBLIC_SITE_URL) {
    return process.env.NEXT_PUBLIC_SITE_URL;
  }
  if (process.env.VERCEL_URL) {
    return `https://${process.env.VERCEL_URL}`;
  }
  return process.env.NODE_ENV === 'production' ? PROD_URL : LOCAL_URL;
}

function withTrailingSlash(url: string) {
  return url.endsWith('/') ? url : `${url}/`;
}

export const siteUrl = new URL(withTrailingSlash(resolveSiteUrl()));
export const docsPathname = DOCS_PATH;
export const docsBaseUrl = new URL(DOCS_PATH, siteUrl).toString();

export function absoluteUrl(pathname: string) {
  return new URL(pathname, siteUrl).toString();
}
