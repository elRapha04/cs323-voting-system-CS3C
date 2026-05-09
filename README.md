# Distributed Voting System
### CS323 – Parallel and Distributed Computing | Final Term Activity

A cloud-based distributed voting system implemented on **Google Cloud Platform**, integrating Edge Nodes, Cloud Run, Pub/Sub, Worker Services, and Firestore to demonstrate real-world distributed computing concepts.

---

## System Architecture

```
Edge Nodes  →  Cloud Run (API)  →  Pub/Sub (Queue)  →  Worker Service  →  Firestore (DB)
```

| Component | Role |
|---|---|
| Edge Nodes | Generate and send vote requests |
| Cloud Run | Receives incoming vote API requests |
| Pub/Sub | Acts as message buffer between API and worker |
| Worker Service | Processes messages from the queue |
| Firestore | Stores final vote results |

---

## Key Concepts Demonstrated

- **Parallel processing** – multiple votes handled simultaneously instead of one-by-one
- **Asynchronous communication** – messages travel through Pub/Sub before reaching the database
- **Eventual consistency** – Firestore updates complete after short delays, not instantly
- **Fault tolerance** – temporary service failures don't stop the whole system
- **Scalability vs. latency tradeoff** – higher throughput can introduce queue delays at peak load

---

## Team Reflections

### Naduma – Performance and Scalability
During testing, the system worked efficiently with few votes since requests moved quickly through the pipeline. As vote volume increased, Pub/Sub acted as a buffer and kept the system stable. Compared to sequential execution, the distributed setup handled larger workloads much faster. However, increased traffic did introduce delays in Firestore updates due to message accumulation in the queue — showing how scalability improves throughput but can also create latency during peak load.

---

### Romualdez – Deployment and Configuration
The biggest lesson was how important proper cloud configuration is in distributed computing. Integrating Cloud Run, Pub/Sub, the worker service, and Firestore required careful setup of IAM permissions and service connections. Debugging was also harder than a local app because the system was spread across multiple cloud services. Once properly configured though, all components operated independently and reliably even under temporary delays.

---

### Jurial – Distributed Communication Behavior
Testing gave practical insight into how asynchronous communication works in distributed environments. Votes did not appear in Firestore immediately because messages first passed through Pub/Sub and the worker service. Unlike sequential systems, the distributed setup processed votes asynchronously, meaning some updates appeared later than others. Pub/Sub effectively buffered incoming requests during rapid vote generation, preventing the worker service from being overloaded.

---

### Espina – Failure Recovery and Reliability
The distributed architecture proved more reliable and resilient than centralized execution. During testing, some services experienced temporary delays, yet the system kept functioning because Pub/Sub stored pending messages until the worker service could process them. In a sequential system, a failure in one part stops everything. Here, Firestore updates were eventually completed even after short delays, demonstrating eventual consistency and the value of message queues in maintaining system stability.

---

### Palongpalong – Practical Learning Experience
Before this activity, distributed computing was mainly theoretical. Observing the actual interaction between all components made the concepts much clearer. Distributed execution improved the system's ability to handle multiple simultaneous requests, but it also made debugging harder since tasks ran asynchronously across several services. Monitoring logs from different services became necessary to trace where issues originated. The activity showed how distributed systems offer better scalability and flexibility for handling larger workloads.

---

## Observations Summary

| Scenario | Behavior Observed |
|---|---|
| Low traffic | Fast, smooth pipeline from edge to Firestore |
| High traffic | Pub/Sub buffered excess messages; slight Firestore delay |
| Service delay | System stayed online; messages queued until processed |
| vs. Sequential | Distributed was significantly faster at scale |

---

## Tech Stack

- Google Cloud Run
- Google Pub/Sub
- Google Firestore
- Python / Node.js Worker Service
- GCP IAM & Service Accounts
