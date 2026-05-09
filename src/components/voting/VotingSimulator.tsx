import { useEffect, useMemo, useReducer, useRef, useState } from "react";

type Choice = "A" | "B" | "C";

interface Vote {
  id: string;
  voteId: string;
  userId: string;
  pollId: string;
  choice: Choice;
  edgeId: string;
  tCreated: number;
  tIngested?: number;
  tProcessed?: number;
}

type Stage = "edge" | "api" | "pubsub" | "worker" | "firestore";

interface InFlight {
  id: string;
  vote: Vote;
  stage: Stage;
  bornAt: number;
}

interface State {
  votes: Record<string, Vote>; // by docId user_pollId (idempotent store = "Firestore")
  edgeSent: number;
  apiAccepted: number;
  pubsubBacklog: InFlight[];
  pubsubBuffered: number;
  workerProcessed: number;
  inFlight: InFlight[];
  duplicates: number;
  log: { ts: number; level: "info" | "warn" | "error"; text: string }[];
  edgeNodes: { id: string; sent: number }[];
}

type Action =
  | { type: "tick"; now: number }
  | { type: "edge_send"; vote: Vote }
  | { type: "api_accept"; flight: InFlight }
  | { type: "pubsub_arrive"; flight: InFlight }
  | { type: "worker_pull"; flight: InFlight }
  | { type: "firestore_write"; vote: Vote; isDup: boolean }
  | { type: "set_edge_nodes"; ids: string[] }
  | { type: "reset" }
  | { type: "log"; level: "info" | "warn" | "error"; text: string };

const MAX_LOG = 60;

function pushLog(state: State, level: State["log"][number]["level"], text: string): State {
  return {
    ...state,
    log: [{ ts: Date.now(), level, text }, ...state.log].slice(0, MAX_LOG),
  };
}

const initial: State = {
  votes: {},
  edgeSent: 0,
  apiAccepted: 0,
  pubsubBacklog: [],
  pubsubBuffered: 0,
  workerProcessed: 0,
  inFlight: [],
  duplicates: 0,
  log: [],
  edgeNodes: [
    { id: "edge-1", sent: 0 },
    { id: "edge-2", sent: 0 },
    { id: "edge-3", sent: 0 },
  ],
};

function reducer(state: State, action: Action): State {
  switch (action.type) {
    case "edge_send": {
      const flight: InFlight = {
        id: action.vote.id,
        vote: action.vote,
        stage: "edge",
        bornAt: performance.now(),
      };
      const nodes = state.edgeNodes.map((n) =>
        n.id === action.vote.edgeId ? { ...n, sent: n.sent + 1 } : n,
      );
      return pushLog(
        {
          ...state,
          edgeSent: state.edgeSent + 1,
          inFlight: [...state.inFlight, flight],
          edgeNodes: nodes,
        },
        "info",
        `[${action.vote.edgeId}] vote ${action.vote.userId.slice(0, 6)} → ${action.vote.choice}`,
      );
    }
    case "api_accept": {
      return pushLog(
        {
          ...state,
          apiAccepted: state.apiAccepted + 1,
          inFlight: state.inFlight.map((f) =>
            f.id === action.flight.id ? { ...f, stage: "api" } : f,
          ),
        },
        "info",
        `[api] accepted ${action.flight.vote.userId.slice(0, 6)} → publish`,
      );
    }
    case "pubsub_arrive": {
      return {
        ...state,
        pubsubBacklog: [...state.pubsubBacklog, action.flight],
        pubsubBuffered: state.pubsubBuffered + 1,
        inFlight: state.inFlight.map((f) =>
          f.id === action.flight.id ? { ...f, stage: "pubsub" } : f,
        ),
      };
    }
    case "worker_pull": {
      return {
        ...state,
        pubsubBacklog: state.pubsubBacklog.filter((f) => f.id !== action.flight.id),
        inFlight: state.inFlight.map((f) =>
          f.id === action.flight.id ? { ...f, stage: "worker" } : f,
        ),
      };
    }
    case "firestore_write": {
      const docId = `${action.vote.userId}_${action.vote.pollId}`;
      const wasPresent = !!state.votes[docId];
      const next: State = {
        ...state,
        votes: { ...state.votes, [docId]: { ...action.vote, tProcessed: Date.now() } },
        workerProcessed: state.workerProcessed + 1,
        duplicates: state.duplicates + (wasPresent ? 1 : 0),
        inFlight: state.inFlight.filter((f) => f.vote.id !== action.vote.id),
      };
      return pushLog(
        next,
        wasPresent ? "warn" : "info",
        wasPresent
          ? `[worker] duplicate ${action.vote.userId.slice(0, 6)} — idempotent overwrite`
          : `[worker] stored ${action.vote.userId.slice(0, 6)} (${action.vote.choice})`,
      );
    }
    case "set_edge_nodes": {
      return {
        ...state,
        edgeNodes: action.ids.map((id) => {
          const existing = state.edgeNodes.find((n) => n.id === id);
          return existing ?? { id, sent: 0 };
        }),
      };
    }
    case "reset":
      return { ...initial, edgeNodes: state.edgeNodes.map((n) => ({ ...n, sent: 0 })) };
    case "log":
      return pushLog(state, action.level, action.text);
    default:
      return state;
  }
}

function uid() {
  return Math.random().toString(36).slice(2, 11);
}

export function VotingSimulator() {
  const [state, dispatch] = useReducer(reducer, initial);
  const [running, setRunning] = useState(true);
  const [duplicateSends, setDuplicateSends] = useState(1);
  const [workerDown, setWorkerDown] = useState(false);
  const [apiDown, setApiDown] = useState(false);
  const [edgeCount, setEdgeCount] = useState(3);
  const [rateMs, setRateMs] = useState(900);

  const stateRef = useRef(state);
  stateRef.current = state;

  // Sync edge node count.
  useEffect(() => {
    const ids = Array.from({ length: edgeCount }, (_, i) => `edge-${i + 1}`);
    dispatch({ type: "set_edge_nodes", ids });
  }, [edgeCount]);

  // Edge generation loop.
  useEffect(() => {
    if (!running) return;
    const interval = setInterval(() => {
      stateRef.current.edgeNodes.forEach((node) => {
        // Each edge fires independently with ~70% chance per tick to mimic jitter.
        if (Math.random() > 0.7) return;
        const userId = uid() + uid();
        const baseVote: Omit<Vote, "id" | "voteId"> = {
          userId,
          pollId: "poll_1",
          choice: (["A", "B", "C"] as Choice[])[Math.floor(Math.random() * 3)],
          edgeId: node.id,
          tCreated: Date.now(),
        };
        for (let i = 0; i < duplicateSends; i++) {
          const vote: Vote = { ...baseVote, id: uid(), voteId: uid() };
          dispatch({ type: "edge_send", vote });
          // Network: edge → api (~250ms)
          setTimeout(() => {
            if (apiDown) {
              dispatch({
                type: "log",
                level: "error",
                text: `[edge] ${vote.edgeId} retry — API unreachable`,
              });
              return;
            }
            const flight: InFlight = {
              id: vote.id,
              vote: { ...vote, tIngested: Date.now() },
              stage: "api",
              bornAt: performance.now(),
            };
            dispatch({ type: "api_accept", flight });
            // API → Pub/Sub (~150ms)
            setTimeout(() => dispatch({ type: "pubsub_arrive", flight }), 150);
          }, 250 + Math.random() * 200);
        }
      });
    }, rateMs);
    return () => clearInterval(interval);
  }, [running, duplicateSends, apiDown, rateMs]);

  // Worker pull loop.
  useEffect(() => {
    if (!running) return;
    const interval = setInterval(() => {
      if (workerDown) return;
      const backlog = stateRef.current.pubsubBacklog;
      // Pull up to 3 messages per tick.
      const batch = backlog.slice(0, 3);
      batch.forEach((flight, idx) => {
        setTimeout(() => {
          dispatch({ type: "worker_pull", flight });
          setTimeout(() => {
            dispatch({ type: "firestore_write", vote: flight.vote, isDup: false });
          }, 200);
        }, idx * 80);
      });
    }, 600);
    return () => clearInterval(interval);
  }, [running, workerDown]);

  const tally = useMemo(() => {
    const t: Record<Choice, number> = { A: 0, B: 0, C: 0 };
    Object.values(state.votes).forEach((v) => (t[v.choice] += 1));
    return t;
  }, [state.votes]);

  const totalStored = Object.keys(state.votes).length;
  const maxTally = Math.max(1, ...Object.values(tally));

  return (
    <div className="space-y-6">
      {/* Controls */}
      <div className="rounded-xl border bg-card p-4 md:p-5">
        <div className="flex flex-wrap items-center gap-3 md:gap-5">
          <button
            onClick={() => setRunning((r) => !r)}
            className="rounded-md bg-primary px-4 py-2 text-sm font-semibold text-primary-foreground hover:opacity-90"
          >
            {running ? "Pause" : "Resume"}
          </button>
          <button
            onClick={() => dispatch({ type: "reset" })}
            className="rounded-md border px-4 py-2 text-sm hover:bg-accent"
          >
            Reset
          </button>

          <Divider />

          <Slider label="Edge nodes" value={edgeCount} min={1} max={6} onChange={setEdgeCount} />
          <Slider
            label="Tick (ms)"
            value={rateMs}
            min={300}
            max={2000}
            step={100}
            onChange={setRateMs}
          />
          <Slider
            label="Duplicate sends"
            value={duplicateSends}
            min={1}
            max={4}
            onChange={setDuplicateSends}
          />

          <Divider />

          <Toggle label="API down" value={apiDown} onChange={setApiDown} tone="destructive" />
          <Toggle
            label="Worker down"
            value={workerDown}
            onChange={setWorkerDown}
            tone="destructive"
          />
        </div>
      </div>

      {/* Pipeline visualization */}
      <Pipeline state={state} workerDown={workerDown} apiDown={apiDown} />

      {/* Metrics + tally */}
      <div className="grid gap-4 md:grid-cols-3">
        <Metrics state={state} totalStored={totalStored} />
        <div className="md:col-span-2 rounded-xl border bg-card p-5">
          <div className="flex items-baseline justify-between">
            <h3 className="font-semibold">Firestore tally · poll_1</h3>
            <span className="text-xs text-muted-foreground">
              {totalStored} unique docs · {state.duplicates} duplicate writes absorbed
            </span>
          </div>
          <div className="mt-4 space-y-3">
            {(["A", "B", "C"] as Choice[]).map((c) => (
              <TallyBar key={c} label={c} value={tally[c]} max={maxTally} />
            ))}
          </div>
        </div>
      </div>

      {/* Log */}
      <div className="rounded-xl border bg-card">
        <div className="border-b px-5 py-3 text-sm font-semibold">Event log</div>
        <div className="max-h-64 overflow-auto px-5 py-3 font-mono text-xs space-y-1">
          {state.log.length === 0 && (
            <p className="text-muted-foreground">Waiting for events…</p>
          )}
          {state.log.map((l, i) => (
            <div
              key={i}
              className={
                l.level === "error"
                  ? "text-[color:var(--color-destructive)]"
                  : l.level === "warn"
                    ? "text-[color:var(--color-warning)]"
                    : "text-foreground/80"
              }
            >
              <span className="text-muted-foreground">
                {new Date(l.ts).toLocaleTimeString()}
              </span>{" "}
              {l.text}
            </div>
          ))}
        </div>
      </div>

      <footer className="text-xs text-muted-foreground">
        Real GCP scaffold (Python + Dockerfiles + gcloud commands) lives in{" "}
        <code className="text-foreground">gcp-voting-system/</code>. This page simulates that
        pipeline in your browser so you can demo it without deploying.
      </footer>
    </div>
  );
}

function Divider() {
  return <span className="hidden md:block h-6 w-px bg-border" />;
}

function Slider({
  label,
  value,
  min,
  max,
  step = 1,
  onChange,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  onChange: (v: number) => void;
}) {
  return (
    <label className="flex flex-col gap-1 text-xs">
      <span className="text-muted-foreground">
        {label}: <span className="text-foreground font-mono">{value}</span>
      </span>
      <input
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="w-32 accent-[color:var(--color-primary)]"
      />
    </label>
  );
}

function Toggle({
  label,
  value,
  onChange,
  tone = "primary",
}: {
  label: string;
  value: boolean;
  onChange: (v: boolean) => void;
  tone?: "primary" | "destructive";
}) {
  const active = value;
  const activeColor =
    tone === "destructive"
      ? "bg-[color:var(--color-destructive)] text-[color:var(--color-destructive-foreground)]"
      : "bg-primary text-primary-foreground";
  return (
    <button
      onClick={() => onChange(!value)}
      className={`rounded-md px-3 py-2 text-xs font-semibold border transition ${
        active ? activeColor : "bg-transparent text-muted-foreground hover:bg-accent"
      }`}
    >
      {active ? "● " : "○ "}
      {label}
    </button>
  );
}

function Pipeline({
  state,
  workerDown,
  apiDown,
}: {
  state: State;
  workerDown: boolean;
  apiDown: boolean;
}) {
  return (
    <div className="rounded-xl border bg-card p-5">
      <div className="flex items-stretch gap-2 md:gap-4 overflow-x-auto">
        <Stage
          color="edge"
          title="Edge Nodes"
          subtitle={`${state.edgeNodes.length} active`}
          metric={state.edgeSent}
          metricLabel="sent"
          alive
        />
        <Arrow active={!apiDown} pulse={state.inFlight.some((f) => f.stage === "edge")} />
        <Stage
          color="api"
          title="Cloud Run API"
          subtitle={apiDown ? "DOWN" : "/vote"}
          metric={state.apiAccepted}
          metricLabel="accepted"
          alive={!apiDown}
        />
        <Arrow active={!apiDown} pulse={state.inFlight.some((f) => f.stage === "api")} />
        <Stage
          color="pubsub"
          title="Pub/Sub"
          subtitle="vote-topic → vote-sub"
          metric={state.pubsubBacklog.length}
          metricLabel="backlog"
          alive
          highlight={state.pubsubBacklog.length > 5}
        />
        <Arrow
          active={!workerDown}
          pulse={state.inFlight.some((f) => f.stage === "pubsub" || f.stage === "worker")}
        />
        <Stage
          color="worker"
          title="Worker"
          subtitle={workerDown ? "DOWN" : "pulling"}
          metric={state.workerProcessed}
          metricLabel="processed"
          alive={!workerDown}
        />
        <Arrow active={!workerDown} pulse={state.inFlight.some((f) => f.stage === "worker")} />
        <Stage
          color="firestore"
          title="Firestore"
          subtitle="votes/"
          metric={Object.keys(state.votes).length}
          metricLabel="docs"
          alive
        />
      </div>
    </div>
  );
}

function Stage({
  color,
  title,
  subtitle,
  metric,
  metricLabel,
  alive,
  highlight,
}: {
  color: "edge" | "api" | "pubsub" | "worker" | "firestore";
  title: string;
  subtitle: string;
  metric: number;
  metricLabel: string;
  alive: boolean;
  highlight?: boolean;
}) {
  const c = `var(--color-${color})`;
  return (
    <div
      className={`min-w-[120px] flex-1 rounded-lg border p-3 text-center transition ${
        !alive ? "opacity-40 grayscale" : ""
      } ${highlight ? "ring-2" : ""}`}
      style={{
        borderColor: alive ? c : undefined,
        background: alive ? `color-mix(in oklab, ${c} 10%, transparent)` : undefined,
        boxShadow: highlight ? `0 0 0 0 ${c}` : undefined,
      }}
    >
      <div
        className="mx-auto mb-2 h-2.5 w-2.5 rounded-full"
        style={{
          background: alive ? c : "var(--color-muted-foreground)",
          color: c,
          animation: alive ? "pulse-ring 1.6s ease-out infinite" : undefined,
        }}
      />
      <div className="text-sm font-semibold leading-tight">{title}</div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mt-0.5">
        {subtitle}
      </div>
      <div className="mt-3 font-mono text-xl font-bold" style={{ color: alive ? c : undefined }}>
        {metric}
      </div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {metricLabel}
      </div>
    </div>
  );
}

function Arrow({ active, pulse }: { active: boolean; pulse: boolean }) {
  return (
    <div className="flex items-center justify-center min-w-[24px]">
      <div
        className={`h-0.5 w-full rounded ${active ? "bg-foreground/30" : "bg-destructive/40"} relative overflow-hidden`}
      >
        {active && pulse && (
          <div
            className="absolute top-0 h-full w-3 bg-[color:var(--color-primary)]"
            style={{
              animation: "flow 1s linear infinite",
              ["--flow-distance" as string]: "100%",
            }}
          />
        )}
      </div>
    </div>
  );
}

function Metrics({ state, totalStored }: { state: State; totalStored: number }) {
  const lost = Math.max(0, state.edgeSent - state.apiAccepted - state.inFlight.filter(f => f.stage === "edge").length);
  return (
    <div className="rounded-xl border bg-card p-5">
      <h3 className="font-semibold">System metrics</h3>
      <dl className="mt-4 space-y-2 text-sm">
        <Row label="Votes generated (edge)" value={state.edgeSent} />
        <Row label="Accepted by API" value={state.apiAccepted} />
        <Row label="Pub/Sub backlog" value={state.pubsubBacklog.length} accent={state.pubsubBacklog.length > 5} />
        <Row label="Worker processed" value={state.workerProcessed} />
        <Row label="Firestore unique docs" value={totalStored} />
        <Row label="Duplicate writes (deduped)" value={state.duplicates} muted />
        <Row label="Lost in transit" value={lost} muted />
      </dl>
    </div>
  );
}

function Row({
  label,
  value,
  accent,
  muted,
}: {
  label: string;
  value: number;
  accent?: boolean;
  muted?: boolean;
}) {
  return (
    <div className="flex items-center justify-between">
      <dt className={muted ? "text-muted-foreground" : ""}>{label}</dt>
      <dd
        className={`font-mono ${accent ? "text-[color:var(--color-warning)] font-bold" : ""} ${muted ? "text-muted-foreground" : ""}`}
      >
        {value}
      </dd>
    </div>
  );
}

function TallyBar({ label, value, max }: { label: string; value: number; max: number }) {
  const pct = (value / max) * 100;
  return (
    <div>
      <div className="flex justify-between text-xs mb-1">
        <span className="font-semibold">Choice {label}</span>
        <span className="font-mono">{value}</span>
      </div>
      <div className="h-3 rounded-full bg-muted overflow-hidden">
        <div
          className="h-full rounded-full transition-all duration-500"
          style={{
            width: `${pct}%`,
            background: `linear-gradient(90deg, var(--color-primary), var(--color-firestore))`,
          }}
        />
      </div>
    </div>
  );
}
