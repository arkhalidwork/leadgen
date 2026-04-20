# Google Maps Session Data Model (Industry-Grade)

This model supports:

- Two-stage processing (`extract` → `contacts`)
- Live partial persistence while jobs are running
- Resume/restart by `session_id`
- Preview/download from persisted data even when crawler is still running

## ER Diagram

```mermaid
erDiagram
    USERS ||--o{ GMAPS_SESSIONS : owns
    GMAPS_SESSIONS ||--o{ GMAPS_SESSION_LEADS : contains
    GMAPS_SESSIONS ||--o{ GMAPS_SESSION_LOGS : emits

    USERS {
        int id PK
        string email
    }

    GMAPS_SESSIONS {
        string session_id PK
        int user_id FK
        string keyword
        string place
        int max_leads
        string phase
        string extraction_status
        string contacts_status
        string status
        int progress
        string message
        int results_count
        datetime created_at
        datetime updated_at
        datetime finished_at
    }

    GMAPS_SESSION_LEADS {
        int id PK
        string session_id FK
        int user_id FK
        string lead_uid "UNIQUE(session_id, lead_uid)"
        string business_name
        string owner_name
        string phone
        string website
        string email
        string address
        string rating
        string reviews
        string category
        string latitude
        string longitude
        string facebook
        string instagram
        string twitter
        string linkedin
        string youtube
        string tiktok
        string pinterest
        string stage
        int is_complete
        json payload
        datetime created_at
        datetime updated_at
    }

    GMAPS_SESSION_LOGS {
        int id PK
        string session_id FK
        int user_id FK
        string phase
        int progress
        string message
        string log_hash "UNIQUE(session_id, log_hash)"
        datetime created_at
    }
```

## Why this works

- `gmaps_sessions` stores lifecycle and control-plane state for each run.
- `gmaps_session_leads` stores each lead as an upserted row using `(session_id, lead_uid)`.
- `gmaps_session_logs` stores append-only runtime events for observability.
- APIs can read from memory first, then durable DB fallback for resilience.
- Download/preview uses persisted leads, so list remains available during contact crawling.
