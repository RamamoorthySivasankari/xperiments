# Gold Rate Analysis & Prediction App — Production-Grade System Architecture
## India-Focused Commodity Intelligence Platform

---

## Context

Indian gold pricing is driven by at least three compounding variables simultaneously: the international XAU/USD spot, the USD/INR forex rate, and India-specific duty/GST/seasonal demand — all changing independently and asynchronously. This system ingests heterogeneous financial data streams, processes them through a layered ML/AI pipeline, and delivers real-time analysis and multi-horizon predictions to web and mobile consumers. The architecture also integrates an LLM-powered daily narrative analysis engine based on the four-layer analysis framework defined in `system_design.md`.

---

## 1. Architecture Overview

### 1.1 High-Level Data Flow

```
External Sources (MCX, COMEX, LBMA, RBI, FRED, NewsAPI, Twitter/X)
    │
    ▼
Data Ingestion Layer (REST pollers, WebSocket connectors, batch crawlers)
    │
    ▼
Kafka Message Bus (topic-partitioned by data class and priority)
    │
    ▼
Stream Processing (Apache Flink — micro-batch + CEP for alert triggers)
    │
    ▼
Feature Store + Time-Series DB (Feast on TimescaleDB + Redis)
    │
    ▼
ML Ensemble Engine (ARIMA + Prophet + LSTM + XGBoost in parallel)
    │
    ▼
AI Analysis Engine (Claude API + structured 4-layer prompt pipeline)
    │
    ▼
Backend Microservices (FastAPI gateways + gRPC internal comms)
    │
    ▼
API Gateway (Kong) + WebSocket Hub (Socket.io)
    │
    ▼
Frontend Consumers (React Web Dashboard + React Native Mobile App)
```

### 1.2 Architectural Patterns

| Pattern | Why Used |
|---|---|
| **Lambda Architecture** | Speed layer for sub-second intraday ticks + batch layer for multi-year model training; serving layer merges both |
| **Event-Driven (Kafka)** | Decouples producers/consumers, enables replay for audit/debugging, horizontally scalable per topic |
| **CQRS** | Separates write path (price ingestion, alert config) from read path (dashboard, predictions); eliminates tick-write vs dashboard-read contention |
| **Microservices with Domain Boundaries** | Each domain (pricing, prediction, portfolio, alerts, analysis, user) owns its DB and API contract; gRPC sync + Kafka async |
| **Sidecar / Service Mesh (Istio)** | mTLS, circuit breaking, retries, and distributed tracing without polluting business logic |

---

## 2. Data Sources & Ingestion Layer

### 2.1 Gold Price Sources

| Source | Ingestion Method | Frequency | Tier | Notes |
|---|---|---|---|---|
| **MCX** (Multi Commodity Exchange) | WebSocket (FIX adapter) + REST poll fallback | Tick / 1-min OHLCV | Tier-1 | GOLDM & GOLD contracts, OI, bid/ask/LTP; market hours 09:00–23:30 IST |
| **IBJA** (India Bullion & Jewellers Assoc) | REST API + web scrape fallback | 2x daily (12:30 & 17:00 IST) | Tier-1 | Authoritative India physical spot — 24K & 22K per 10g |
| **LBMA** (London Bullion Market Assoc) | REST API (AM/PM fix) | 2x daily + real-time proxy | Tier-1 | Global benchmark; AM fix ~10:30 London, PM fix ~15:00 |
| **COMEX** (CME Group) | CME DataMine REST + CME STP gateway | Real-time during CME hours | Tier-1 | GC futures, OI, volume, COT (weekly Friday) |
| **XAU/USD Spot** | Metals-API + Yahoo Finance `yfinance` | 1-minute | Tier-1 | International spot price |
| **NCDEX** | REST poll | 5-minute | Tier-2 | Cross-reference for MCX divergence |

### 2.2 Forex Sources

| Source | Pairs | Frequency | Tier |
|---|---|---|---|
| RBI Reference Rate | USD/INR (official daily fix) | Daily 12:30 IST | Tier-1 |
| Yahoo Finance | USD/INR real-time, EUR/USD, GBP/USD | 1-minute | Tier-1 |
| Alpha Vantage FX API | Cross-rates for DXY composition | 5-minute | Tier-2 |
| Quandl / ICE Futures | DXY (US Dollar Index) | 1-minute during NY hours | Tier-1 |
| Fixer.io / Open Exchange Rates | Fallback forex | Daily | Tier-3 |

### 2.3 Macro Economic Indicators

| Source | Data Points | Frequency | Tier |
|---|---|---|---|
| **FRED API** (St. Louis Fed) | Fed Funds Rate, US 10Y/2Y/30Y Treasury yields, CPI, PPI, PCE, M2, ISM | Per publication schedule | Tier-1 |
| **RBI** (rbi.org.in + data.rbi.org.in) | Repo rate, CRR, SLR, RBI gold reserves, INR reference rates | 6x/year policy + daily forex | Tier-1 |
| **World Bank API** | Global inflation, India GDP projections, EM indices | Monthly/quarterly | Tier-3 |
| **US Treasury** (fiscaldata.treasury.gov) | 2Y, 10Y, 30Y daily yields | Daily | Tier-1 |

### 2.4 ETF & Institutional Flows

| Source | Data | Frequency | Tier |
|---|---|---|---|
| SPDR Gold Shares (GLD) — State Street scraper | AUM, shares outstanding delta (flow proxy), NAV | Daily post-market | Tier-2 |
| iShares Gold Trust (IAU) — BlackRock scraper | AUM, shares outstanding, NAV | Daily | Tier-2 |
| NSE India (GOLDBEES, SBI Gold ETF) | NAV, AUM, units outstanding | Daily | Tier-2 |
| CFTC COT Report | Commercial/non-commercial long/short, net speculative positioning on COMEX | Weekly (Friday, T+3 lag) | Tier-2 |

### 2.5 Sentiment Sources

| Source | Ingestion | Frequency | Tier |
|---|---|---|---|
| NewsAPI | REST poll, keywords: "gold", "MCX gold", "Federal Reserve", "inflation", "COMEX" | Every 15 minutes | Tier-2 |
| Twitter/X | X API v2 Filtered Stream: #gold, #MCXGold, #AkshayaTritiya, XAU | Real-time stream | Tier-3 |
| Reddit | PRAW library: r/gold, r/india, r/IndiaInvestments, r/personalfinanceindia | Every 30 minutes | Tier-3 |
| Economic Calendar | Investing.com scrape + Forex Factory API: upcoming Fed, CPI, RBI MPC dates | Daily refresh; T-24h event alert | Tier-2 |

### 2.6 India Seasonal Demand Database

Custom-built festive calendar in PostgreSQL:
- **Events**: Diwali/Dhanteras (Oct-Nov), Akshaya Tritiya (Apr-May), Gudi Padwa, Pongal, Navratri, Eid, peak wedding months (Nov-Dec, Apr-May)
- **Schema**: `{date, festive_name, demand_score (0.0–1.0), duration_days, historical_avg_premium_pct}`
- Updated annually by ops; queried daily by ML feature pipeline

### 2.7 Other Data Sources

| Source | Data | Frequency | Tier |
|---|---|---|---|
| EIA + Yahoo Finance (`CL=F`) | WTI Crude Oil | 1-minute NYMEX hours | Tier-2 |
| ICE Futures + Quandl | Brent Crude | 1-minute ICE hours | Tier-2 |
| World Gold Council API + IMF IFS | RBI, PBOC, ECB, Fed gold reserve holdings (monthly tonnes) | Monthly | Tier-2 |

### 2.8 Ingestion Reliability Tiers

| Tier | SLA | Fallback |
|---|---|---|
| Tier-1 | 99.5% | Secondary source auto-switchover + dead-letter queue + ops alert (PagerDuty) |
| Tier-2 | 98% | Retry with exponential backoff; use last-known-good cached value |
| Tier-3 | 95% | Drop and log; no ops alert |

---

## 3. Streaming & Messaging Architecture

### 3.1 Kafka Cluster Design

- **3 brokers** across 3 availability zones (KRaft mode — no ZooKeeper, Kafka 3.6+)
- Replication factor: 3 (Tier-1 topics), 2 (Tier-2/3)
- `min.insync.replicas=2` for Tier-1 topics
- Schema registry: Confluent Schema Registry (Avro schemas for all topics)

### 3.2 Topic Design

| Topic | Partitions | Retention | Partition Key | Description |
|---|---|---|---|---|
| `gold.prices.raw` | 12 | 7d | `{source}:{instrument}` | Raw tick/OHLCV from all gold price sources |
| `gold.prices.normalized` | 12 | 7d | `{instrument}:{currency}` | Normalized, deduplicated, INR-adjusted |
| `gold.prices.aggregated` | 6 | 30d | `{instrument}:{timeframe}` | OHLCV bars (1m, 5m, 15m, 1H, 1D) |
| `forex.stream` | 8 | 3d | `{pair}` | USD/INR, EUR/USD, DXY real-time |
| `macro.events` | 4 | 30d | `{source}:{indicator}` | Fed decisions, CPI, RBI announcements |
| `macro.indicators` | 4 | 90d | `{indicator}` | FRED, World Bank batch releases |
| `etf.flows` | 4 | 90d | `{fund}` | Daily ETF AUM and flow data |
| `sentiment.raw` | 8 | 3d | `{source}` | Raw news/tweets/Reddit posts |
| `sentiment.scored` | 8 | 7d | `{source}` | BERT-scored sentiment + magnitude |
| `sentiment.aggregated` | 4 | 30d | `{window}` | Hourly/daily aggregated sentiment |
| `technical.indicators` | 6 | 7d | `{instrument}:{tf}` | RSI, EMA, MACD, Bollinger computed values |
| `cot.data` | 2 | 180d | `{contract}` | Weekly CFTC COT positioning |
| `crude.oil.prices` | 4 | 7d | `{contract}` | WTI/Brent real-time |
| `central.bank.reserves` | 2 | 365d | `{bank}` | Monthly CB gold holdings |
| `seasonal.signals` | 2 | 365d | `{date}` | Festive season demand scoring |
| `predictions.output` | 6 | 30d | `{model}:{horizon}` | ML prediction results |
| `analysis.generated` | 2 | 90d | `{date}:{type}` | LLM daily analysis JSON |
| `alerts.triggers` | 6 | 7d | `{user_id}` | Alert threshold breach events |
| `alerts.outbound` | 6 | 3d | `{channel}:{user_id}` | Email/SMS/push dispatch queue |
| `alerts.dlq` | 2 | 14d | - | Dead letter queue for failed sends |
| `model.retraining.triggers` | 2 | 30d | `{model}` | Retraining trigger events |
| `audit.events` | 4 | 365d | `{service}:{user_id}` | Full regulatory audit trail |

### 3.3 Consumer Groups

| Consumer Group | Topics Consumed | Service |
|---|---|---|
| `price-normalizer` | `gold.prices.raw`, `forex.stream` | gold-price-service |
| `bar-aggregator` | `gold.prices.normalized` | gold-price-service |
| `technical-engine` | `gold.prices.aggregated` | data-processing-service |
| `sentiment-scorer` | `sentiment.raw` | sentiment-service |
| `feature-builder` | `gold.prices.aggregated`, `forex.stream`, `macro.indicators`, `sentiment.aggregated`, `technical.indicators`, `seasonal.signals` | ml-feature-service |
| `alert-evaluator` | `gold.prices.normalized`, `predictions.output` | alert-service |
| `alert-dispatcher` | `alerts.outbound` | alert-service |
| `analysis-trigger` | `gold.prices.aggregated`, `macro.events`, `sentiment.aggregated` | analysis-service |
| `prediction-ingestor` | `predictions.output` | prediction-service |
| `audit-writer` | `audit.events` | audit-service |

### 3.4 Dead Letter Queue & Retry Strategy

- Failed Kafka consumer processing → publish to `alerts.dlq` with original message + error metadata
- Retry worker: exponential backoff (1s, 2s, 4s, 8s, max 3 retries) then escalate to PagerDuty
- Idempotency: All Kafka consumers use Kafka Transactions + idempotent producers; dedup key = `{source}:{instrument}:{timestamp_ms}`

### 3.5 Apache Flink vs Spark Streaming Decision

**Chosen: Apache Flink** for real-time stream processing
- Lower latency (true event-time streaming vs micro-batch)
- Native Complex Event Processing (CEP) for alert rules (e.g., "price drops 2% in 5 minutes")
- Better state management for sliding window aggregations

**Apache Spark**: retained for batch layer only — historical feature computation, model training data prep, backfill jobs

---

## 4. Data Processing Pipeline

### 4.1 Pipeline Stages

```
Stage 1: RAW INGESTION
  ↓ Deduplication, schema validation, source tagging
Stage 2: NORMALIZATION
  ↓ Currency conversion to INR, unit standardization (per 10g), timezone → IST
Stage 3: ENRICHMENT
  ↓ MCX vs IBJA divergence computation, USD/INR adjusted price, GST/customs overlay
Stage 4: FEATURE ENGINEERING
  ↓ Technical indicators, lag features, seasonal encoding, sentiment merge
Stage 5: FEATURE STORE WRITE
  ↓ Feast (online store → Redis, offline store → TimescaleDB)
Stage 6: MODEL INPUT VECTORS
  ↓ Per-model feature selection, normalization, windowing
```

### 4.2 Feature Engineering Details

**Technical Indicators** (computed by Flink on `gold.prices.aggregated`):
- RSI (14-period) on XAU/USD and MCX Gold
- EMA 50, EMA 200, EMA 20 (short-term momentum)
- MACD (12/26/9) with histogram
- Bollinger Bands (20-period, ±2σ)
- ATR (14-period) for volatility
- VWAP (intraday, reset at MCX open)

**Lag Features** (for LSTM/XGBoost):
- Gold price returns: 1h, 4h, 24h, 7d, 30d, 90d
- USD/INR change: 1h, 24h, 7d
- DXY change: 1h, 24h
- Sentiment score rolling average: 1h, 24h, 7d

**Seasonality Encoding**:
- Days to next festive event, days since last festive event
- Is_festive_month (binary), festive_demand_score (0.0–1.0)
- Day-of-week, week-of-year, month sine/cosine encoding

**Macro Features**:
- US 10Y-2Y yield spread (inverted yield curve signal)
- Real yield (10Y nominal - CPI)
- Fed rate surprise delta (announced vs expected)
- RBI repo rate level and delta

### 4.3 Data Quality Checks

- Schema validation via Avro on Kafka ingestion
- Price spike detection: Z-score > 5σ from 24h rolling mean → quarantine + ops alert
- Stale data detection: if source hasn't updated in N minutes (Tier-1: 5min, Tier-2: 60min) → fallback source trigger
- Completeness checks: batch job validates no gaps in OHLCV bars for critical instruments

---

## 5. Storage Architecture

### 5.1 TimescaleDB — Time-Series Store

**Primary hypertables:**

```sql
-- Gold prices (all sources, normalized)
CREATE TABLE gold_prices (
  time         TIMESTAMPTZ NOT NULL,
  instrument   TEXT,        -- 'MCX_GOLD', 'XAU_USD', 'IBJA_24K', etc.
  source       TEXT,
  open         NUMERIC(12,4),
  high         NUMERIC(12,4),
  low          NUMERIC(12,4),
  close        NUMERIC(12,4),
  volume       BIGINT,
  currency     TEXT DEFAULT 'INR',
  timeframe    TEXT         -- '1m', '5m', '1H', '1D'
);
SELECT create_hypertable('gold_prices', 'time', chunk_time_interval => INTERVAL '1 day');

-- Technical indicators
CREATE TABLE technical_indicators (
  time         TIMESTAMPTZ NOT NULL,
  instrument   TEXT,
  timeframe    TEXT,
  rsi_14       NUMERIC(6,2),
  ema_20       NUMERIC(12,4),
  ema_50       NUMERIC(12,4),
  ema_200      NUMERIC(12,4),
  macd         NUMERIC(12,4),
  macd_signal  NUMERIC(12,4),
  bb_upper     NUMERIC(12,4),
  bb_lower     NUMERIC(12,4),
  atr_14       NUMERIC(12,4)
);

-- Macro indicators
CREATE TABLE macro_indicators (
  time         TIMESTAMPTZ NOT NULL,
  indicator    TEXT,        -- 'US_10Y_YIELD', 'FED_RATE', 'USD_INR', 'CPI_US'
  source       TEXT,
  value        NUMERIC(12,4),
  unit         TEXT
);
```

**Continuous aggregates** (auto-refresh every 5 minutes):
```sql
CREATE MATERIALIZED VIEW gold_ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket,
       instrument, first(open,time), max(high), min(low), last(close,time), sum(volume)
FROM gold_prices WHERE timeframe = '1m'
GROUP BY bucket, instrument;
```

**Retention policies**: 1-minute data → 90 days; hourly → 3 years; daily → indefinite

### 5.2 PostgreSQL — Transactional Store

Databases and key tables:

```
users_db:
  users (id, email, phone, name, created_at, kyc_status)
  user_sessions (id, user_id, jwt_token_hash, expires_at)
  user_preferences (user_id, notification_channels, default_currency, locale)

portfolio_db:
  holdings (id, user_id, gold_type, quantity_grams, avg_buy_price_inr, buy_date, notes)
  transactions (id, user_id, holding_id, type, grams, price_per_gram, total_inr, timestamp)

alerts_db:
  alert_configs (id, user_id, type, condition, threshold, channels, is_active)
  alert_history (id, config_id, triggered_at, price_at_trigger, notification_sent)

predictions_db:
  prediction_results (id, model_id, horizon, instrument, predicted_price, lower_bound, upper_bound, confidence, created_at)
  model_registry (id, model_name, version, artifact_uri, metrics_json, deployed_at)
  backtests (id, model_id, start_date, end_date, mape, rmse, directional_accuracy)

analysis_db:
  daily_analysis (id, date, sentiment_score, summary, full_json, llm_model_used, created_at)
  macro_events_calendar (id, event_name, event_date, impact_level, actual_value, forecast_value)

seasonal_db:
  festive_calendar (id, date, event_name, demand_score, duration_days, historical_premium_pct)
```

### 5.3 Redis — Caching Layer

| Key Pattern | TTL | Content |
|---|---|---|
| `gold:price:current:{instrument}` | 10s | Latest normalized price (INR) |
| `gold:price:mcx:live` | 5s | MCX live tick stream object |
| `prediction:{model}:{horizon}` | 5min | Latest prediction result |
| `analysis:daily:{date}` | 1h | Today's LLM analysis JSON |
| `sentiment:score:current` | 15min | Aggregated sentiment score |
| `technical:{instrument}:{tf}` | 30s | Latest indicator values |
| `session:{user_id}` | 24h | JWT session + user context |
| `alert:cooldown:{user_id}:{alert_id}` | configurable | Prevent duplicate alerts |
| `feature:vector:{instrument}:{ts}` | 1min | Latest feature vector for fast inference |

### 5.4 InfluxDB — System Metrics

- Kafka consumer lag, ingestion throughput per source, Flink job latency
- ML model inference latency, prediction confidence histograms
- API gateway request rates, error rates, p99 latency
- Alert dispatch success/failure rates

### 5.5 Elasticsearch — News & Sentiment Search

- Index: `gold-news-{YYYY-MM}` (rolling monthly indices)
- Document: `{id, source, url, headline, body, sentiment_score, published_at, tags[]}`
- Used for: full-text search, date-range filtering, aggregations for sentiment trends

### 5.6 Data Lake — MinIO / S3

- Raw data archival: all Kafka topic data exported daily via Kafka Connect S3 Sink
- ML training datasets (Parquet format, partitioned by date/instrument)
- Model artifacts (MLflow artifact store)
- Generated PDF reports archive

---

## 6. ML/AI Engine

### 6.1 Feature Store

**Technology**: Feast (open-source) with:
- Online store → Redis (low-latency inference)
- Offline store → TimescaleDB / Parquet on MinIO (training)

**Feature Views**:
- `gold_price_features`: OHLCV, returns at multiple lags
- `technical_features`: RSI, EMA, MACD, Bollinger
- `macro_features`: yields, Fed rate, RBI rate, CPI
- `forex_features`: USD/INR, DXY, EUR/USD
- `sentiment_features`: news score, Twitter score, aggregated
- `seasonal_features`: demand score, days_to_festive, is_festive_month
- `etf_flow_features`: GLD flows, GOLDBEES flows, COT positioning

### 6.2 Model Architecture

**ARIMA/SARIMA** — Univariate baseline
- Input: MCX Gold 1D close price (5 years history)
- SARIMA(p,d,q)(P,D,Q,s=5) for weekly seasonality + trend
- Horizon: 1D, 7D predictions
- Purpose: Robust trend + seasonality baseline; interpretable

**Facebook Prophet** — India Seasonality Model
- Input: MCX/IBJA daily close + custom Indian festive calendar as regressors
- Additional regressors: USD/INR daily, sentiment score daily
- Handles: Non-linear trends, multiple seasonalities (weekly, monthly, festive)
- Horizon: 7D, 30D, 90D

**LSTM / Transformer** — Multivariate Deep Learning
- Input: 60-day sliding window of 35 features (all feature views above)
- Architecture: Bidirectional LSTM (128→64 units) + attention mechanism OR Temporal Fusion Transformer (TFT)
- Multi-horizon output heads: 30min, 24h, 7D, 30D simultaneously
- Training: PyTorch; inference: TorchServe
- Uncertainty: Monte Carlo Dropout for confidence intervals

**XGBoost/LightGBM** — Feature-Rich Tabular Model
- Input: 150+ engineered features (single-row, no sequence)
- Target: Next-period return (classification: up/down/flat + regression: price)
- Shap values computed for explainability
- Horizon: 24h, 7D
- Retraining: Weekly with last 3 years of data + walk-forward CV

**BERT Sentiment Model**
- Base: `ProsusAI/finbert` (financial domain pre-trained)
- Fine-tuned on gold/commodity-specific Indian financial news corpus
- Output: sentiment score (-1.0 to +1.0) + confidence per article/tweet
- Batch inference via Hugging Face pipeline

### 6.3 Ensemble Weighting Strategy

```
Final Prediction = Σ(wᵢ × model_i_prediction)

Dynamic weights based on rolling 30-day MAPE per horizon:
  w_arima     = 0.15 (base)
  w_prophet   = 0.25 (higher for 7D+ due to seasonality)
  w_lstm      = 0.35 (higher for intraday/24h)
  w_xgboost   = 0.25 (balanced)

Weight adjustment: Softmax normalization of inverse MAPE scores
If model MAPE degrades > 2x rolling average → auto-reduce weight + trigger retraining alert
```

### 6.4 Prediction Horizons & Outputs

| Horizon | Models Used | Update Frequency | Confidence Interval |
|---|---|---|---|
| Intraday (30min) | LSTM, XGBoost | Every 30 minutes | 80% CI |
| 24-hour | All 4 models | Hourly | 80% CI |
| 1-week | Prophet, LSTM, XGBoost | Daily | 70% CI |
| 3-month | Prophet, ARIMA, XGBoost | Weekly | 60% CI |
| 12-month | ARIMA, Prophet | Monthly | 50% CI |

### 6.5 MLflow Pipeline

- **Experiment tracking**: all training runs logged (parameters, metrics, artifacts)
- **Model registry**: staging → production promotion workflow
- **Artifact store**: MinIO S3-compatible backend
- **Retraining triggers**:
  1. Scheduled: weekly (full retrain), daily (incremental fine-tune for LSTM)
  2. Event-driven: model MAPE > threshold → publish to `model.retraining.triggers`
  3. Manual: ops dashboard trigger
- **Serving**: MLflow Model Serving → FastAPI wrapper for low-latency inference

### 6.6 Backtesting Framework

- Walk-forward validation: 80/20 split with 30-day forward windows
- Metrics: MAPE, RMSE, MAE, Directional Accuracy %, Sharpe-like return ratio
- Shadow deployment: new model runs in shadow (predictions logged but not served) for 7 days before promotion
- Results stored in `backtests` table in PostgreSQL

### 6.7 Scenario Analysis

- **Bull scenario**: DXY -5%, geopolitical risk +2, Fed dovish pivot, festive season active
- **Base scenario**: current macro conditions extrapolated
- **Bear scenario**: DXY +5%, Fed hawkish, global risk-off sell-off
- Each scenario: Monte Carlo simulation (500 paths) over prediction horizon
- Output: price distribution, P5/P50/P95 bands, probability of each scenario

---

## 7. AI Analysis Engine

### 7.1 LLM Integration (Claude API)

**Model**: `claude-sonnet-4-6` (balance of speed + quality for daily reports)
- Fallback: `claude-haiku-4-5-20251001` for rapid intraday alerts

**Trigger**: Daily at 07:00 IST (pre-market) + 18:30 IST (post-market summary)

### 7.2 Structured Prompt Pipeline

The 4-layer analysis from `system_design.md` is operationalized as a structured data assembly pipeline:

```python
# Layer 1: Global Macro
macro_context = {
  "fed_rate": current_fed_rate,
  "fed_rate_delta": last_change,
  "us_10y_yield": current_yield,
  "us_2y_yield": current_2y,
  "yield_spread": spread,
  "cpi_us": latest_cpi,
  "dxy": current_dxy,
  "dxy_7d_change_pct": dxy_change
}

# Layer 2: International Liquidity
liquidity_context = {
  "gld_flow_7d_bn_usd": etf_flow,
  "comex_oi_change": oi_delta,
  "cot_net_speculative": cot_position,
  "geopolitical_events": recent_events_list,
  "fear_sentiment_score": computed_score  # 1-10
}

# Layer 3: India Domestic
india_context = {
  "usd_inr": current_rate,
  "usd_inr_7d_change_pct": change,
  "customs_duty_pct": 15.0,  # current India import duty
  "gst_pct": 3.0,
  "seasonal_demand_score": festive_score,
  "active_festive": festive_event_name,
  "mcx_vs_ibja_premium": premium_inr
}

# Layer 4: Technical
technical_context = {
  "xau_usd": current_spot,
  "mcx_gold_ltp": mcx_price,
  "rsi_14_daily": rsi,
  "ema_50": ema50,
  "ema_200": ema200,
  "support_levels": [s1, s2],
  "resistance_levels": [r1, r2],
  "trend": "BULLISH" | "BEARISH" | "SIDEWAYS"
}

# ML Predictions
predictions = {
  "24h_target_inr": predicted_price,
  "24h_range": [low, high],
  "7d_target_inr": predicted_7d,
  "confidence": confidence_pct,
  "scenario_bull": bull_price,
  "scenario_bear": bear_price
}
```

**System prompt**: Verbatim from `system_design.md` (4-layer financial analyst persona)

**Output format** (structured JSON + prose):
```json
{
  "sentiment_score": 7,
  "executive_summary": "...",
  "global_headwinds": "...",
  "global_tailwinds": "...",
  "domestic_impact": "...",
  "price_24h_24k_mcx": {"target": 95200, "support": 94500, "resistance": 96000},
  "price_7d_24k_mcx": {"target": 96500, "support": 93000, "resistance": 98000},
  "price_24h_retail": {"target": 93800, "support": 93000, "resistance": 95000},
  "risk_factors": ["...", "..."],
  "upcoming_events": [{"event": "US CPI Release", "date": "2026-06-12", "impact": "HIGH"}]
}
```

### 7.3 Output Parsing & Storage

- JSON schema validation on LLM output (retry with correction prompt on parse failure)
- Stored in `analysis_db.daily_analysis`
- Served via `/api/v1/analysis/daily` and `/api/v1/analysis/latest`
- Cached in Redis (TTL: 1h)

---

## 8. Backend Microservices

### 8.1 Service Catalog

| Service | Responsibility | Stack | DB | Scaling |
|---|---|---|---|---|
| `gold-price-service` | Real-time price aggregation, normalization, INR conversion | FastAPI + Python | TimescaleDB + Redis | HPA 2–10 pods, CPU-based |
| `prediction-service` | Serve ML predictions, model metadata | FastAPI + Python | PostgreSQL + Redis | HPA 2–8 pods |
| `analysis-service` | Orchestrate LLM analysis generation, store results | FastAPI + Python | PostgreSQL + Redis | 1–3 pods (rate-limited by LLM API) |
| `alert-service` | Threshold monitoring, notification dispatch | FastAPI + Python | PostgreSQL + Redis | HPA 2–6 pods |
| `portfolio-service` | User gold holdings, P&L computation | FastAPI + Python | PostgreSQL | 2–4 pods |
| `user-service` | Auth (JWT), profile, preferences | FastAPI + Python | PostgreSQL | 2–4 pods |
| `data-ingestion-service` | Orchestrate all data source pulls, publish to Kafka | Python + Celery | Redis (Celery broker) | 2–8 pods per source type |
| `sentiment-service` | BERT inference on raw text, publish scored sentiment | FastAPI + Python | Elasticsearch | 2–4 pods (GPU node pool for BERT) |
| `reporting-service` | Generate PDF/Excel reports | FastAPI + Python | PostgreSQL + MinIO | 1–2 pods |
| `ml-feature-service` | Feature store read/write, feature vector assembly | FastAPI + Python | Feast (Redis + TimescaleDB) | 2–6 pods |

### 8.2 Key Internal API Contracts

**gold-price-service → Kafka**:
- Consumes: `gold.prices.raw`, `forex.stream`
- Publishes: `gold.prices.normalized`, `gold.prices.aggregated`

**prediction-service gRPC** (internal):
```protobuf
service PredictionService {
  rpc GetPrediction(PredictionRequest) returns (PredictionResponse);
  rpc GetLatestPredictions(InstrumentRequest) returns (PredictionsListResponse);
}
```

**alert-service Celery tasks**:
- `evaluate_alerts`: runs every 30 seconds, consumes from Redis
- `dispatch_email`: SendGrid async task
- `dispatch_sms`: MSG91/Twilio async task
- `dispatch_push`: FCM async task

---

## 9. API Design

### 9.1 REST API (Kong API Gateway)

**Base URL**: `https://api.goldanalysis.in/v1`

**Authentication**: JWT Bearer tokens + API key for third-party integrations

**Key Endpoints**:

```
# Prices
GET  /prices/current              → MCX + IBJA current prices (24K, 22K per 10g INR)
GET  /prices/history?instrument=MCX_GOLD&from=&to=&tf=1D
GET  /prices/comparison           → MCX vs IBJA vs International spread

# Predictions
GET  /predictions/latest?horizon=24h|1W|3M|12M
GET  /predictions/scenarios       → Bull/base/bear with probabilities

# Analysis
GET  /analysis/daily?date=        → Full LLM analysis for a date
GET  /analysis/latest             → Today's latest analysis
GET  /analysis/sentiment          → Current sentiment score + breakdown

# Technical
GET  /technical/indicators?instrument=MCX_GOLD&tf=1D
GET  /technical/levels            → Support/resistance levels

# Macro
GET  /macro/indicators            → Fed rate, yields, DXY, USD/INR
GET  /macro/events                → Upcoming high-impact events calendar
GET  /macro/etf-flows             → Latest ETF flow data

# Alerts (authenticated)
GET    /alerts                    → User's alert configurations
POST   /alerts                    → Create alert
PUT    /alerts/{id}               → Update alert
DELETE /alerts/{id}               → Delete alert
GET    /alerts/history            → Past alert triggers

# Portfolio (authenticated)
GET    /portfolio/holdings        → User holdings + current value
POST   /portfolio/holdings        → Add holding
PUT    /portfolio/holdings/{id}   → Update holding
DELETE /portfolio/holdings/{id}   → Remove holding
GET    /portfolio/performance     → P&L, CAGR, vs benchmark

# Reports
GET    /reports/daily             → Download daily PDF report
GET    /reports/portfolio         → Download portfolio report
POST   /reports/custom            → Generate custom date range report

# Auth
POST   /auth/register
POST   /auth/login
POST   /auth/refresh
POST   /auth/logout
```

### 9.2 WebSocket (Socket.io)

**Endpoint**: `wss://ws.goldanalysis.in`

**Rooms/Events**:
```
room: 'mcx-live'     → emit: 'price-tick'     (every MCX tick)
room: 'forex-live'   → emit: 'forex-update'   (USD/INR every 30s)
room: 'alerts-{uid}' → emit: 'alert-triggered' (personal alert events)
room: 'analysis'     → emit: 'analysis-ready'  (when new LLM analysis generated)
room: 'sentiment'    → emit: 'sentiment-update' (every 15 minutes)
```

### 9.3 Rate Limiting (Kong)

| Tier | Limit | Window |
|---|---|---|
| Free | 100 requests | per hour |
| Pro | 10,000 requests | per hour |
| Enterprise (API key) | 100,000 requests | per hour |
| WebSocket connections | 1 per user | concurrent |

---

## 10. Frontend Architecture

### 10.1 React Web Dashboard

**Stack**: React 18 + TypeScript + Vite + TanStack Query + Zustand + TailwindCSS

**Chart Library**: TradingView Lightweight Charts (gold price OHLCV with technical overlays) + Recharts (non-price analytics charts)

**Key Screens**:

```
/ Dashboard
  ├── Live MCX price card (WebSocket)
  ├── IBJA spot price card (22K / 24K)
  ├── USD/INR card
  ├── Market Sentiment Score widget (1-10)
  ├── Mini TradingView chart (MCX 1D)
  └── Quick prediction widget (24h, 1W)

/charts
  ├── Full TradingView chart (MCX/XAU — multi-timeframe)
  ├── Technical indicator overlays (RSI, EMA, MACD, Bollinger)
  ├── MCX vs IBJA vs XAU comparison chart
  └── USD/INR correlation overlay

/predictions
  ├── Multi-horizon prediction cards (30min, 24h, 1W, 3M, 12M)
  ├── Confidence intervals visualization
  ├── Ensemble model breakdown (which model contributes how much)
  └── Scenario analysis (bull/base/bear fan chart)

/analysis
  ├── Today's LLM analysis (executive summary, sentiment score)
  ├── Global headwinds/tailwinds breakdown
  ├── India domestic impact section
  ├── Upcoming risk events calendar
  └── Historical analysis archive

/portfolio
  ├── Holdings table (quantity, buy price, current value, P&L)
  ├── Total portfolio value vs gold price chart
  ├── CAGR calculation
  └── Add/edit/remove holding forms

/alerts
  ├── Active alerts list
  ├── Create alert form (price threshold, prediction change, macro event)
  ├── Alert history timeline
  └── Notification preferences

/macro
  ├── Global macro dashboard (DXY, yields, Fed rate)
  ├── ETF flows chart (GLD, IAU, GOLDBEES)
  ├── COT positioning chart
  └── Central bank reserves chart
```

**State Management**:
- Zustand: global UI state (selected instrument, timeframe, user session)
- TanStack Query: all server data (auto-refetch intervals per data freshness requirements)
- WebSocket: price ticks and alerts (native Socket.io client)

### 10.2 React Native Mobile App

**Stack**: React Native 0.74 + Expo + TypeScript + React Navigation + MMKV (offline storage)

**Key Screens**: Dashboard (condensed), Price Chart, Quick Prediction, Portfolio, Alerts, Analysis Summary

**Push Notifications**: Firebase Cloud Messaging (FCM) via Expo Notifications

**Offline Support**: Last-known prices and latest analysis cached in MMKV; stale data banner shown when offline

---

## 11. Notification & Alert System

### 11.1 Alert Types

| Type | Trigger | Priority |
|---|---|---|
| Price Threshold | MCX/IBJA price crosses user-set threshold (above/below) | High |
| Price Change % | Price moves > N% in M minutes | High |
| Prediction Update | Model confidence band changes significantly | Medium |
| Macro Event | Upcoming high-impact event T-24h warning | Medium |
| Daily Digest | Morning report (07:00 IST) | Low |
| Portfolio Alert | Holding value change > N% | Medium |
| Sentiment Shift | Market sentiment score crosses threshold | Low |

### 11.2 Multi-Channel Dispatch

| Channel | Provider | India Optimization |
|---|---|---|
| Email | SendGrid | HTML + plain text; IST-aware scheduling |
| SMS | MSG91 (primary, India-optimized) + Twilio (fallback) | DLT-registered sender ID for India |
| Push (Web) | Web Push API (VAPID) | Service worker in React web app |
| Push (Mobile) | Firebase Cloud Messaging (FCM) via Expo | Background + foreground handling |

### 11.3 Alert Deduplication & Cooldown

- Redis key `alert:cooldown:{user_id}:{alert_id}` with configurable TTL (default 30 minutes)
- If alert re-triggers within cooldown → suppress; log to history but don't send notification
- Daily digest: aggregate all triggered alerts into single email instead of individual sends

---

## 12. Infrastructure & Deployment

### 12.1 Kubernetes Cluster Design

**Cloud**: AWS (primary) — Mumbai region (ap-south-1) for India latency + Singapore (ap-southeast-1) for DR

**Node Pools**:
| Pool | Instance Type | Min/Max | Workload |
|---|---|---|---|
| `general` | c6i.xlarge (4 vCPU, 8GB) | 3/20 | FastAPI microservices |
| `ml-cpu` | c6i.2xlarge (8 vCPU, 16GB) | 2/8 | XGBoost, ARIMA, Prophet training |
| `ml-gpu` | g4dn.xlarge (1x T4 GPU) | 1/4 | LSTM/BERT inference |
| `data` | r6i.2xlarge (8 vCPU, 64GB) | 3/6 | Flink, Kafka, TimescaleDB |
| `spot` | c6i.xlarge (spot) | 0/10 | Batch Spark jobs, training |

**Namespaces**:
```
production/     → live user-facing services
staging/        → pre-production testing
data-platform/  → Kafka, Flink, TimescaleDB, Redis
ml-platform/    → MLflow, model training, feature store
monitoring/     → Prometheus, Grafana, Jaeger, ELK
```

### 12.2 CI/CD Pipeline

```
GitHub → GitHub Actions CI:
  1. Lint (ruff/eslint)
  2. Unit tests (pytest/jest)
  3. Build Docker image
  4. Push to ECR
  5. Run integration tests
  6. Security scan (Trivy)
    ↓
ArgoCD (GitOps):
  - Watches Helm chart repo
  - Auto-sync to staging on main branch merge
  - Manual promotion to production (requires 2 approvals)
  - Rollback: ArgoCD history-based instant rollback
```

### 12.3 Helm Chart Structure

```
charts/
├── gold-price-service/
├── prediction-service/
├── analysis-service/
├── alert-service/
├── portfolio-service/
├── user-service/
├── data-ingestion-service/
├── sentiment-service/
├── kafka/             (Bitnami Kafka chart)
├── timescaledb/       (TimescaleDB Helm chart)
├── redis/             (Bitnami Redis chart)
├── flink/             (Apache Flink Helm chart)
└── monitoring/        (kube-prometheus-stack)
```

### 12.4 Observability Stack

| Layer | Tool | Purpose |
|---|---|---|
| Metrics | Prometheus + Grafana | Service health, Kafka lag, model latency |
| Tracing | Jaeger (via Istio) | Distributed request tracing end-to-end |
| Logs | ELK Stack (Elasticsearch + Logstash + Kibana) | Centralized structured log search |
| Alerting | Alertmanager → PagerDuty + Slack | Ops on-call alerts |
| Uptime | Prometheus Blackbox Exporter | External endpoint health checks |

**Key Grafana Dashboards**:
- Gold Price Pipeline Health (ingestion lag, source availability)
- ML Model Performance (MAPE over time, prediction vs actual)
- API Gateway (request rate, error rate, latency p50/p99)
- Kafka Consumer Lag per topic
- Alert System (dispatch success rates by channel)

### 12.5 Auto-Scaling Policies

```yaml
# HPA example for gold-price-service
spec:
  minReplicas: 2
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: External  # Kafka consumer lag
    external:
      metric:
        name: kafka_consumer_lag
        selector:
          matchLabels:
            topic: gold.prices.raw
      target:
        type: AverageValue
        averageValue: 1000
```

### 12.6 Disaster Recovery

- **RTO**: 4 hours (full recovery), 15 minutes (failover to DR region)
- **RPO**: 5 minutes (Kafka replication to DR), 1 hour (database snapshots)
- **DB Backup**: TimescaleDB + PostgreSQL → hourly snapshots to S3; cross-region replication
- **Kafka MirrorMaker 2**: continuous topic replication to Singapore DR cluster

---

## 13. Security

### 13.1 Data Security

| Layer | Control |
|---|---|
| Transit | TLS 1.3 everywhere; mTLS between services via Istio |
| At Rest | AES-256 encryption for RDS/S3/EBS; TimescaleDB transparent encryption |
| Secrets | HashiCorp Vault (dynamic DB credentials, API keys injected as env vars) |
| Keys | AWS KMS for envelope encryption |

### 13.2 API Security

- **WAF**: AWS WAF in front of Kong API Gateway (OWASP rule set)
- **Rate Limiting**: Kong rate-limiting plugin (per-user, per-IP, per-API-key)
- **Input Validation**: Pydantic models on all FastAPI endpoints (no raw user input to DB)
- **SQL Injection**: SQLAlchemy ORM with parameterized queries; no raw SQL with user input
- **Authentication**: JWT (RS256 algorithm, 15-minute access token + 7-day refresh token)
- **CORS**: Strict allowlist of frontend origins

### 13.3 SEBI & RBI Compliance

- All financial data served is informational / analysis-only; no trading execution (no SEBI broker registration required for current scope)
- Disclaimers: All prediction pages include SEBI-mandated "past performance ≠ future results" disclaimer
- Data retention: User data retained per IT Act 2000 and RBI digital guidelines
- PII: Phone numbers and emails stored encrypted; masked in logs

### 13.4 India Data Localization

- All user PII and financial data stored in AWS ap-south-1 (Mumbai) per PDPB (Personal Data Protection Bill) guidelines
- DR region (Singapore) stores only encrypted backups, not live PII

---

## 14. Implementation Roadmap

### Phase 1 — Infrastructure & Data Ingestion Foundation (Months 1-2)
**Deliverables**:
- Kubernetes cluster setup (EKS) + namespaces + Helm chart scaffolding
- Kafka cluster (KRaft) with all topics created and schema registry running
- Data ingestion service: MCX WebSocket connector, LBMA REST, IBJA scraper, FRED API, RBI scraper, Yahoo Finance forex
- TimescaleDB + PostgreSQL provisioned, hypertables created
- Redis cluster deployed
- Basic CI/CD pipeline (GitHub Actions + ArgoCD)
- Tier-1 data sources flowing into Kafka and stored in TimescaleDB

**Team**: 2 backend engineers, 1 DevOps/Platform engineer

### Phase 2 — Data Pipeline & Feature Engineering (Months 3-4)
**Deliverables**:
- Apache Flink jobs: price normalization, bar aggregation, technical indicator computation
- ETF flow scrapers, sentiment data ingestion (NewsAPI, Twitter/X, Reddit)
- Elasticsearch deployed + sentiment indexing
- Feature store (Feast) set up with all 6 feature views
- Economic calendar integration
- Indian festive calendar database populated
- Crude oil, COT data ingestion
- Data quality monitoring (Z-score spike detection, stale data alerts)

**Team**: 2 backend engineers, 1 data engineer

### Phase 3 — ML Model Training & Serving (Months 5-6)
**Deliverables**:
- Historical data backfill (5 years of OHLCV, macro, forex)
- ARIMA/SARIMA and Prophet models trained and validated
- LSTM model trained on GPU nodes; TorchServe deployed
- XGBoost/LightGBM trained with Shap explainability
- BERT sentiment model fine-tuned on gold news corpus
- Ensemble weighting logic implemented
- MLflow model registry + retraining pipeline
- prediction-service deployed, serving all horizons
- Backtesting framework running + baseline accuracy benchmarks

**Team**: 2 ML engineers, 1 backend engineer

### Phase 4 — AI Analysis Engine & Backend APIs (Months 7-8)
**Deliverables**:
- Claude API integration in analysis-service
- 4-layer structured data assembly pipeline
- Daily analysis generation (07:00 IST + 18:30 IST)
- All REST API endpoints implemented (FastAPI)
- WebSocket server (Socket.io) for live price streaming
- Kong API Gateway configured (rate limiting, auth, routing)
- Alert service fully operational (all channels: email/SMS/push)
- Portfolio service + user service deployed
- JWT auth flow complete

**Team**: 2 backend engineers, 1 ML engineer

### Phase 5 — Frontend, Mobile & Notifications (Months 9-10)
**Deliverables**:
- React web dashboard (all screens)
- TradingView Lightweight Charts integration
- React Native mobile app (iOS + Android) via Expo
- Push notification integration (FCM)
- MSG91 SMS integration for India
- PDF report generation service
- User onboarding flow (register, set alerts, add holdings)
- Full end-to-end testing (Playwright for web, Detox for mobile)

**Team**: 2 frontend engineers, 1 mobile engineer

### Phase 6 — Optimization, Compliance & Launch (Months 11-12)
**Deliverables**:
- Full observability stack (Prometheus + Grafana dashboards, Jaeger, ELK)
- Performance tuning (TimescaleDB continuous aggregates, Redis warming)
- Security hardening (Vault integration, WAF rules, penetration testing)
- SEBI/RBI compliance review + legal disclaimers
- India data localization audit
- Load testing (k6) — target: 10,000 concurrent dashboard users
- A/B testing framework for model improvements
- Soft launch (beta users), feedback loop, production launch

**Team**: Full team + 1 security engineer + 1 legal/compliance consultant

---

## 15. Technology Stack Summary

| Layer | Component | Technology | Why |
|---|---|---|---|
| **Frontend Web** | Dashboard | React 18 + TypeScript + Vite | Fast HMR, strong typing, ecosystem |
| **Frontend Web** | Charts | TradingView Lightweight Charts | Purpose-built financial charts |
| **Frontend Web** | State | Zustand + TanStack Query | Lightweight global state + server state |
| **Mobile** | App | React Native + Expo | Code sharing with web, OTA updates |
| **API Gateway** | Gateway | Kong | Plugin ecosystem, rate limiting, auth |
| **Real-time** | WebSocket | Socket.io (Node.js) | Rooms, namespaces, reconnection |
| **Backend** | Microservices | FastAPI (Python) | Async, Pydantic, auto-docs, ML-friendly |
| **Messaging** | Broker | Apache Kafka (KRaft) | Durable, replayable, partitioned streams |
| **Stream Processing** | Real-time | Apache Flink | True event-time, CEP for alert rules |
| **Batch Processing** | ETL | Apache Spark | Large-scale historical feature computation |
| **Feature Store** | Store | Feast | Online (Redis) + offline (TimescaleDB) |
| **ML Training** | Framework | PyTorch + scikit-learn | LSTM/BERT (PyTorch), XGBoost/ARIMA (sklearn) |
| **ML Serving** | Inference | TorchServe + FastAPI | LSTM via TorchServe; others via FastAPI |
| **ML Tracking** | Experiment | MLflow | Model registry, artifact store, retraining |
| **AI Analysis** | LLM | Claude API (claude-sonnet-4-6) | Best financial reasoning + structured output |
| **Time-Series DB** | Storage | TimescaleDB | Hypertables, continuous aggregates, compression |
| **Relational DB** | Storage | PostgreSQL | Users, portfolio, alerts, model metadata |
| **Cache** | In-Memory | Redis 7 | Price cache, session, feature vectors, cooldown |
| **Search** | Full-Text | Elasticsearch 8 | News/sentiment search, aggregations |
| **Metrics DB** | Observability | InfluxDB | System + pipeline metrics |
| **Data Lake** | Archival | MinIO (S3-compatible) | Raw data archival, ML training Parquet |
| **Notifications** | Email | SendGrid | Deliverability, templates |
| **Notifications** | SMS (India) | MSG91 | DLT-registered, India-optimized |
| **Notifications** | Push | Firebase Cloud Messaging | Web + mobile push, reliable |
| **Orchestration** | Container | Kubernetes (EKS) | Auto-scaling, self-healing, cloud-native |
| **GitOps** | CD | ArgoCD | Declarative deployments, rollback |
| **Service Mesh** | mTLS/Traffic | Istio | Circuit breaking, tracing, mTLS |
| **Secrets** | Management | HashiCorp Vault | Dynamic secrets, API key rotation |
| **Monitoring** | Metrics | Prometheus + Grafana | Industry standard, Kafka/Flink integrations |
| **Tracing** | Distributed | Jaeger | End-to-end request traces via Istio |
| **Logging** | Centralized | ELK Stack | Structured log aggregation and search |
| **Cloud** | Provider | AWS (ap-south-1 Mumbai) | India latency, PDPB compliance |
| **DR** | Region | AWS (ap-southeast-1 Singapore) | Cross-region failover |

---

## Verification Plan

1. **Data Pipeline**: Verify Kafka topics receiving messages from all Tier-1 sources; check TimescaleDB hypertable row counts growing in real-time
2. **ML Predictions**: Run backtests on 1-year holdout; MAPE < 3% for 24h horizon (baseline target)
3. **AI Analysis**: Trigger manual analysis generation; validate JSON schema, sentiment score in 1-10 range, price bands reasonable vs actual MCX prices
4. **WebSocket**: Connect to `mcx-live` room; verify price ticks arriving < 5s during MCX market hours
5. **Alerts**: Create test alert at current MCX price + 1%; manually push test price; verify email + push notification received within 30s
6. **API Gateway**: Load test with k6 (1,000 RPS); verify rate limiting kicks in above tier limits; JWT auth on protected endpoints
7. **Portfolio**: Add test holding, verify P&L calculation matches (quantity × current_price - quantity × buy_price)
8. **DR Failover**: Simulate ap-south-1 outage; verify traffic routes to Singapore within 15 minutes
