export default function DocsLoading() {
  return (
    <div className="mx-auto w-full max-w-5xl px-6 py-20">
      <div className="animate-pulse space-y-6">
        <div className="h-6 w-56 rounded bg-fd-accent" />
        <div className="space-y-3">
          <div className="h-4 w-full rounded bg-fd-accent" />
          <div className="h-4 w-5/6 rounded bg-fd-accent" />
          <div className="h-4 w-2/3 rounded bg-fd-accent" />
        </div>
        <div className="space-y-3">
          <div className="h-4 w-full rounded bg-fd-accent" />
          <div className="h-4 w-4/6 rounded bg-fd-accent" />
          <div className="h-4 w-3/6 rounded bg-fd-accent" />
        </div>
      </div>
    </div>
  );
}

