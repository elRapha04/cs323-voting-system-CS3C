import { createFileRoute } from "@tanstack/react-router";
import { VotingSimulator } from "@/components/voting/VotingSimulator";

export const Route = createFileRoute("/")({
  component: Index,
  head: () => ({
    meta: [
      { title: "Distributed Voting System — Edge → Cloud Run → Pub/Sub → Firestore" },
      {
        name: "description",
        content:
          "Interactive simulation of a fault-tolerant distributed voting system on GCP with edge nodes, Pub/Sub messaging, and Firestore storage.",
      },
    ],
  }),
});

function Index() {
  return (
    <main className="min-h-screen px-4 py-8 md:px-8 md:py-12">
      <div className="mx-auto max-w-7xl">
        <header className="mb-8">
          <p className="text-xs font-mono tracking-widest text-[color:var(--color-primary)] uppercase">
            CS323 · Distributed Systems Lab
          </p>
          <h1 className="mt-2 text-3xl md:text-5xl font-bold tracking-tight">
            Distributed Voting System
          </h1>
          <p className="mt-3 text-sm md:text-base text-muted-foreground max-w-3xl">
            Live simulation of an edge-to-cloud pipeline:
            <span className="text-[color:var(--color-edge)] font-medium"> Edge Nodes</span> →
            <span className="text-[color:var(--color-api)] font-medium"> Cloud Run API</span> →
            <span className="text-[color:var(--color-pubsub)] font-medium"> Pub/Sub</span> →
            <span className="text-[color:var(--color-worker)] font-medium"> Worker</span> →
            <span className="text-[color:var(--color-firestore)] font-medium"> Firestore</span>.
            Inject faults to observe buffering, idempotency, and recovery.
          </p>
        </header>
        <VotingSimulator />
      </div>
    </main>
  );
}
