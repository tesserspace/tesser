# Tesser Docs

This folder contains the standalone documentation site for the Tesser trading framework.  
The site is powered by [Fumadocs](https://fumadocs.dev) and deployed separately (e.g. Cloudflare Pages) so it does not interfere with the Rust workspace.

## Local Development

```bash
cd docs
pnpm install
pnpm dev
```

Visit `http://localhost:3000/` for the landing page and `http://localhost:3000/docs` for the documentation root.  
In production the base URL automatically switches to `https://tesser.space`.

## Project Structure

| Path | Description |
| --- | --- |
| `app/(home)` | Marketing/overview landing page. |
| `app/docs` | Documentation layout and MDX routes. |
| `app/api/search` | Server-side search index. |
| `app/og` | Dynamic Open Graph image routes. |
| `content/docs` | MDX sources plus `meta.json` ordering. |
| `lib` | Shared helpers (site metadata, nav config, source loader). |

## Scripts

| Script | Purpose |
| --- | --- |
| `pnpm dev` | Run the dev server with HMR. |
| `pnpm build` | Create a production build for deployment. |
| `pnpm start` | Preview the production build locally. |
| `pnpm types:check` | Regenerate MDX artifacts and run `tsc --noEmit`. |

The Next.js build output (`.next`) can be uploaded to Cloudflare Pages or any host that supports static Next.js exports.
