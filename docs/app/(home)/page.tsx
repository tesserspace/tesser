'use client';

import { Suspense } from 'react';
import { Canvas } from '@react-three/fiber';
import { Loader, useProgress } from '@react-three/drei';
import { CrystalScene } from './components/crystal-scene';
import { UIOverlay } from './components/ui-overlay';

export default function HomePage() {
  return (
    <div className="relative w-full min-h-screen bg-black text-white selection:bg-blue-500/30">
      <div className="fixed inset-0 z-0">
        <div className="absolute top-[-20%] right-[-10%] w-[800px] h-[800px] bg-indigo-900/20 rounded-full blur-[120px] pointer-events-none mix-blend-screen" />
        <div className="absolute bottom-[-20%] left-[-10%] w-[600px] h-[600px] bg-fuchsia-900/20 rounded-full blur-[100px] pointer-events-none mix-blend-screen" />
        <Canvas shadows dpr={[1, 2]} gl={{ antialias: true, toneMappingExposure: 2.5 }}>
          <Suspense fallback={null}>
            <CrystalScene />
          </Suspense>
        </Canvas>
      </div>

      <div className="relative z-10 w-full h-full overflow-x-hidden">
        <UIOverlay />
      </div>

      <SceneLoader />
    </div>
  );
}

function SceneLoader() {
  const { errors } = useProgress();
  const hasErrors = errors.length > 0;

  if (hasErrors) {
    return (
      <div className="pointer-events-none fixed inset-x-0 bottom-6 z-50 flex justify-center px-6">
        <div className="max-w-xl rounded-full border border-zinc-800 bg-black/80 px-6 py-3 text-center text-sm text-zinc-300 shadow-lg backdrop-blur">
          Unable to load the interactive background because of a blocked asset download.
          The documentation is still fully browsable—please refresh when you have network
          access to re-enable the 3D scene.
        </div>
      </div>
    );
  }

  return (
    <Loader
      containerStyles={{ background: 'transparent', pointerEvents: 'none' }}
      innerStyles={{ background: '#2563eb', height: 2 }}
      barStyles={{ background: '#e879f9' }}
      dataInterpolation={(p) => `Initializing Tesser... ${p.toFixed(0)}%`}
    />
  );
}
