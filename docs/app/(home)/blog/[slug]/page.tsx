import type { Metadata } from 'next';
import { notFound } from 'next/navigation';
import Link from 'next/link';
import { PathUtils } from 'fumadocs-core/source';
import { blog } from '@/lib/source';
import { getMDXComponents } from '@/mdx-components';

type BlogPostPageProps = {
  params: Promise<{ slug: string }>;
};

function resolveDate(path: string, value?: string | Date) {
  if (value) return new Date(value);

  const filename = PathUtils.basename(path, PathUtils.extname(path));
  const match = filename.match(/\d{4}-\d{2}-\d{2}/);

  return new Date(match?.[0] ?? '1970-01-01');
}

export default async function BlogPostPage(
  props: BlogPostPageProps,
) {
  const params = await props.params;
  const page = blog.getPage([params.slug]);

  if (!page) notFound();

  const { body: Mdx } = await page.data.load();
  const publishedAt = resolveDate(page.path, page.data.date as string | Date);
  const author =
    typeof page.data.author === 'string'
      ? page.data.author
      : 'Tesser Core Team';
  const description =
    typeof page.data.description === 'string'
      ? page.data.description
      : undefined;

  return (
    <article className="relative isolate mx-auto w-full max-w-3xl px-6 py-32 text-white">
      <div className="pointer-events-none absolute inset-0 -z-10 opacity-60">
        <div className="absolute inset-0 bg-gradient-to-b from-blue-600/10 via-black to-black blur-3xl" />
        <div className="absolute left-[-20%] top-10 h-[420px] w-[420px] rounded-full bg-purple-700/30 blur-[140px]" />
      </div>

      <Link
        href="/blog"
        className="inline-flex items-center gap-2 rounded-full border border-white/20 px-4 py-1 text-sm text-white transition hover:bg-white hover:text-black focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-white/60"
      >
        ← Back to Dispatches
      </Link>

      <p className="mt-8 text-xs font-mono uppercase tracking-[0.4em] text-blue-300">
        {publishedAt.toLocaleDateString(undefined, {
          month: 'short',
          day: 'numeric',
          year: 'numeric',
        })}
      </p>
      <h1 className="mt-4 text-4xl font-semibold leading-tight text-white">
        {page.data.title}
      </h1>
      <p className="mt-3 text-lg text-zinc-400">{description}</p>
      <p className="mt-6 text-sm text-zinc-500">
        By <span className="font-semibold text-white">{author}</span>
      </p>

      <div className="prose prose-invert mt-12 max-w-none prose-headings:text-white prose-strong:text-white prose-a:text-blue-300">
        <Mdx components={getMDXComponents()} />
      </div>
    </article>
  );
}

export async function generateMetadata(
  props: BlogPostPageProps,
): Promise<Metadata> {
  const params = await props.params;
  const page = blog.getPage([params.slug]);

  if (!page) notFound();

  return {
    title: `${page.data.title} | Tesser Blog`,
    description:
      (typeof page.data.description === 'string'
        ? page.data.description
        : undefined) ??
      'Dispatches for investors and developers building on Tesser.',
  };
}

export function generateStaticParams(): { slug: string }[] {
  return blog.getPages().map((page) => ({
    slug: page.slugs[0],
  }));
}
