# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:

1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
> The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
>
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go would be preferred.

You may use any data store of your choosing, with `sqlite` being one example. Like our own code, we expect the code structure to make it easy to switch data stores.

Please note that Google Pub/Sub is best effort ordering and we want to keep the latest scan. While the example scanner does not publish scans at a rate where this would be an issue, we expect the application to be able to handle extreme out of orderness. Consider what would happen if the application received a scan that is 24 hours old.

cmd/scanner/main.go should not be modified

---

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account. This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don't hesitate to ask any follow-up questions for clarification.

---

## Solution

### Architecture Overview

The solution implements a scan data processor that:

1. **Consumes messages** from Google Pub/Sub subscription `scan-sub`
2. **Processes both V1 and V2 formats** - decodes base64 for V1, uses plain string for V2
3. **Stores records** in a pluggable data store (SQLite by default)
4. **Handles out-of-order messages** using timestamp comparison in atomic upsert operations
5. **Uses at-least-once semantics** - ACKs only after successful DB write

```
┌─────────┐     ┌─────────┐     ┌───────────┐     ┌───────────┐
│ Scanner │ --> │ Pub/Sub │ --> │ Processor │ --> │   Store   │
└─────────┘     └─────────┘     └───────────┘     └───────────┘
                                                   │
                                      ┌────────────┼────────────┐
                                      ▼            ▼            ▼
                                  SQLite      PostgreSQL    Memory
                                 (default)    (scaling)    (testing)
```

### Project Structure

```
mini-scan/
├── cmd/
│   ├── scanner/              # Provided scanner (not modified)
│   └── processor/            # New: Data processor application
│       └── main.go
├── pkg/
│   ├── scanning/             # Existing: Scan types
│   ├── processor/            # New: Message processing & Pub/Sub consumer
│   │   ├── processor.go
│   │   └── processor_test.go
│   └── store/                # New: Pluggable data store
│       ├── store.go          # Interface + factory
│       ├── sqlite.go         # SQLite implementation
│       ├── postgres.go       # PostgreSQL implementation
│       ├── memory.go         # In-memory (for testing)
│       └── store_test.go
├── config/                   # New: Environment configuration
│   ├── .env.example          # Template with all options
│   ├── .env.sqlite           # SQLite config (default)
│   └── .env.postgres         # PostgreSQL config
├── docker-compose.yml        # Updated: includes processor service
├── Dockerfile                # New: Dockerfile for processor
└── IMPLEMENTATION_PLAN.txt   # Detailed implementation notes
```

### Key Design Decisions

1. **Pluggable Store Interface**: Easy to switch between SQLite, PostgreSQL, or in-memory storage by changing environment variables.
2. **Out-of-Order Handling**: Uses atomic conditional upsert with timestamp comparison:

   ```sql
   INSERT ... ON CONFLICT DO UPDATE ... WHERE new_timestamp > existing_timestamp
   ```
3. **Horizontal Scalability**:

   - Stateless processor design
   - Pub/Sub automatically distributes messages across instances
   - PostgreSQL implementation supports concurrent writes from multiple processors
4. **At-Least-Once Semantics**: Messages are ACKed only after successful database write. Duplicate processing is safe due to idempotent upserts.

### Configuration

Environment variables are managed via config files in the `config/` folder:

```
config/
├── .env.example     # Template with all options documented
├── .env.sqlite      # SQLite config (default)
├── .env.postgres    # PostgreSQL config (for scaling)
└── .env.local       # Your local overrides (gitignored)
```

**To switch databases**, edit `docker-compose.yml`:

```yaml
env_file:
  - ./config/.env.sqlite    # Default: SQLite
  # - ./config/.env.postgres  # For horizontal scaling
```


| Environment Variable     | Default          | Description                                  |
| ------------------------ | ---------------- | -------------------------------------------- |
| `PUBSUB_PROJECT_ID`      | `test-project`   | Google Cloud project ID                      |
| `PUBSUB_SUBSCRIPTION_ID` | `scan-sub`       | Pub/Sub subscription name                    |
| `STORE_TYPE`             | `sqlite`         | Store type:`sqlite`, `postgres`, or `memory` |
| `STORE_CONNECTION`       | `/data/scans.db` | Connection string for the store              |

---

## Testing Instructions

### Automated Tests

Run the unit tests:

```bash
go test ./pkg/... -v
```

This tests:

- Store implementations (Memory, SQLite)
- Message processing (V1/V2 formats)
- Out-of-order message handling
- Edge cases (invalid JSON, unknown versions)

### Manual Testing with Docker

1. **Start the full stack**:

   ```bash
   docker compose up --build
   ```
2. **Watch the logs** - you should see:

   - Scanner publishing messages every second
   - Processor consuming and storing records
3. **Query the database** (in another terminal):

   ```bash
   # After a few seconds of running:
   sqlite3 ./data/scans.db "SELECT * FROM service_records ORDER BY last_timestamp DESC LIMIT 10;"
   ```
4. **Verify record count grows**:

   ```bash
   watch -n 2 'sqlite3 ./data/scans.db "SELECT COUNT(*) as count, service FROM service_records GROUP BY service;"'
   ```
5. **Stop with Ctrl+C**

### Testing Out-of-Order Handling

The out-of-order handling can be verified via unit tests:

```bash
go test ./pkg/processor -v -run TestProcessOutOfOrder
```

This test processes messages in order: `1000, 2000, 500, 1500, 3000` and verifies that only `1000`, `2000`, and `3000` are stored (older timestamps are skipped).

### Testing with PostgreSQL (Optional)

To test with PostgreSQL for horizontal scaling:

1. Create `docker-compose.postgres.yml` or modify `docker-compose.yml`:

   ```yaml
   postgres:
     image: postgres:15-alpine
     environment:
       POSTGRES_USER: scanner
       POSTGRES_PASSWORD: scanner
       POSTGRES_DB: scans
     healthcheck:
       test: ["CMD-SHELL", "pg_isready -U scanner -d scans"]
       interval: 5s
       timeout: 5s
       retries: 5

   processor:
     environment:
       STORE_TYPE: postgres
       STORE_CONNECTION: postgres://scanner:scanner@postgres:5432/scans?sslmode=disable
     depends_on:
       postgres:
         condition: service_healthy
   ```
2. Scale processors:

   ```bash
   docker compose up --scale processor=3
   ```

---

## How to Scale in Production

This application is designed for horizontal scalability. Here's how to scale it in a production environment:

### Why It's Horizontally Scalable


| Design Decision                  | How It Enables Scaling                                           |
| -------------------------------- | ---------------------------------------------------------------- |
| **Stateless processors**         | No shared memory state - any instance can process any message    |
| **Pub/Sub message distribution** | Multiple subscribers automatically share the message load        |
| **Idempotent upserts**           | Safe for duplicate/concurrent processing from multiple instances |
| **Pluggable PostgreSQL**         | Handles concurrent writes from many processors                   |
| **Atomic timestamp checks**      | Race conditions resolved at database level                       |

### Production Architecture (GCP Example)

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────────────────┐
│   Scanner   │────▶│  Cloud Pub/Sub  │────▶│    GKE / Cloud Run          │
│  (multiple) │     │  (managed)      │     │  ┌─────────┐ ┌─────────┐   │
└─────────────┘     └─────────────────┘     │  │Processor│ │Processor│   │
                                            │  │   #1    │ │   #2    │...│
                                            │  └────┬────┘ └────┬────┘   │
                                            │       │           │        │
                                            │       ▼           ▼        │
                                            │  ┌─────────────────────┐   │
                                            │  │ Cloud SQL/AlloyDB   │   │
                                            │  │   (PostgreSQL)      │   │
                                            │  └─────────────────────┘   │
                                            └─────────────────────────────┘
```

### Scaling Steps

**1. Switch to PostgreSQL**

```bash
# In docker-compose.yml or Kubernetes configmap:
STORE_TYPE=postgres
STORE_CONNECTION=postgres://user:pass@db-host:5432/scans?sslmode=require
```

**2. Deploy Multiple Instances**

*Docker Compose:*

```bash
docker compose up --scale processor=5
```

*Kubernetes:*

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: scan-processor
spec:
  replicas: 5  # Scale here
  selector:
    matchLabels:
      app: scan-processor
  template:
    spec:
      containers:
      - name: processor
        image: gcr.io/your-project/processor:latest
        envFrom:
        - configMapRef:
            name: processor-config
```

**3. Enable Horizontal Pod Autoscaler (HPA)**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: scan-processor-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: scan-processor
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
```

### Scaling Considerations


| Aspect         | Recommendation                                                            |
| -------------- | ------------------------------------------------------------------------- |
| **Database**   | Use managed PostgreSQL (Cloud SQL, Cloud Spanner) with connection pooling |
| **Pub/Sub**    | Increase`MaxOutstandingMessages` per instance for throughput              |
| **Monitoring** | Track message backlog, processing latency, DB connections                 |
| **Limits**     | Set resource requests/limits to enable effective HPA                      |

### What Happens Under Load

1. **Messages pile up** in Pub/Sub subscription
2. **HPA detects** increased CPU usage
3. **New pods spawn** automatically
4. **Pub/Sub distributes** messages across all instances
5. **PostgreSQL handles** concurrent writes with atomic upserts
6. **Backlog decreases** as processing capacity increases
