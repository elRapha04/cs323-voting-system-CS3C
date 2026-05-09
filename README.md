# Distributed Voting System  
CS323 – Parallel and Distributed Computing

This project is a distributed voting system that simulates how real-world cloud systems handle data using multiple independent components. Instead of processing everything in one place, the system splits responsibilities across different services so they can run in parallel and communicate asynchronously.

---

## System Overview and Architecture

The system follows an event-driven distributed pipeline:

Edge Nodes → Cloud Run API → Pub/Sub → Worker Service → Firestore

Each component plays a specific role in the system:

- Edge Nodes simulate users generating votes independently
- Cloud Run API receives incoming requests and validates them
- Pub/Sub acts as a message queue that buffers data between services
- Worker Service processes queued votes and removes duplicates
- Firestore stores the final processed results

This setup allows the system to handle multiple requests at the same time without relying on a single centralized process. Communication is asynchronous, meaning each service works independently and does not wait for others to finish before continuing. This improves scalability and fault tolerance, but also introduces slight delays due to message queuing and processing.

## Team Reflections

### Naduma – Performance and Scalability  
From a performance standpoint, the system behaved exactly as expected for a distributed setup. At low request volume, everything flowed smoothly and results reached Firestore quickly. The interesting part came during higher load testing — instead of failing, the system stayed functional because Pub/Sub absorbed the incoming traffic. What changed was timing. Firestore updates started to lag slightly due to queued messages. This made the trade-off very clear: distributed systems scale well, but latency becomes more visible when demand spikes.

---

### Romualdez – Deployment and Configuration  
Deploying the system was honestly more challenging than writing the actual code. Most of the difficulty came from making sure all cloud services were correctly connected and had the right permissions. A single missing IAM role or incorrect environment variable would break the whole flow between API, Pub/Sub, and Firestore. It took several iterations of debugging before everything worked properly. Once deployed successfully, though, the system felt very “modular” — each service ran independently, which made the whole architecture easier to reason about after setup.

---

### Jurial – Distributed Communication Behavior  
What stood out to me most was the delay between action and result. When a vote is sent from the edge node, it doesn’t immediately appear in the database — it first passes through Pub/Sub and then the worker. This creates a natural delay that doesn’t exist in normal sequential programs. During stress testing with multiple edge nodes, I noticed Pub/Sub effectively smoothing out bursts of traffic. Instead of overwhelming the worker, messages were queued and processed gradually. It really highlighted how distributed systems rely on asynchronous flow rather than direct execution.

---

### Espina – Failure Recovery and Reliability  
The system’s behavior during failure testing was the most convincing part of the whole experiment. When the worker service was intentionally stopped, the rest of the system continued running normally. Edge nodes still sent votes, and Pub/Sub simply held the messages in queue. After restarting the worker, processing resumed automatically without any data loss. This showed how fault tolerance is built into the architecture itself rather than manually handled. It also demonstrated why message queues are essential in ensuring reliability in distributed systems.

---

### Palongpalong – Practical Learning Experience  
This activity made distributed computing feel a lot less abstract. Before this, concepts like queues, workers, and edge nodes were just theoretical ideas from lectures. But seeing the full pipeline working in real time made it easier to understand how everything connects. One challenge I kept noticing was how hard debugging becomes when everything is asynchronous — logs are scattered across services, so you can’t rely on a single execution flow anymore. Still, it clearly showed why distributed systems are used: they handle scale better, even if they are more complex to manage.