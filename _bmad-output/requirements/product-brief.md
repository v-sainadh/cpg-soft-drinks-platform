# FreshSip Beverages — CPG Executive Dashboard: Product Brief

**Version:** 1.0  
**Date:** 2026-04-05  
**Author:** BMad Product Owner Agent  
**Status:** Draft — Pending CEO Review  
**Input:** CEO business requirement (2026-04-05), Brainstorming Session 2026-04-05-001  

---

## 1. Opportunity Statement

FreshSip Beverages operates across 12 US states with 3 distribution channels and 4 product categories but currently has no unified view of operational performance. Executive decisions on inventory, production scheduling, and customer account management are made with day-old or week-old data from disconnected ERP reports and spreadsheets.

**The opportunity:** Build a near-real-time executive dashboard — powered by a Medallion data platform on Databricks — that gives leadership the ability to see, in one place, how the business is performing across Sales, Inventory, Production, Distribution, and Customers. This platform will serve as both a daily operational tool and a board presentation showcase demonstrating AI-accelerated data engineering as a competitive capability.

**Business impact estimate (illustrative):**
- Preventing 1 major stockout event per quarter: ~$50K–200K in avoided lost sales
- Identifying one underperforming distribution route: 5–15% logistics cost reduction in that lane
- Early batch yield failure detection: reduce scrapped batches by 10–20%

---

## 2. Target Users

### Primary Users

| User | Role | Primary Need | Pain Today |
|---|---|---|---|
| **CEO** | Executive decision-maker | Holistic view of business health; board presentation | No unified real-time view; relies on manual reports |
| **VP Sales** | Revenue accountability | Revenue by product/region/channel; customer rankings | Slow POS data; no trend comparisons |
| **VP Operations** | Supply chain and production oversight | Inventory levels, yield rates, fulfillment rates | Disconnected ERP and production data |
| **Supply Chain Manager** | Day-to-day inventory and logistics | Reorder alerts, days-of-supply, delivery performance | Manual spreadsheet tracking |

### Secondary Users (Phase 2)

- **Board Members** — read-only board presentation view (static export or shared link)
- **Regional Sales Managers** — filtered view by their assigned states

---

## 3. The Platform Vision

```
Data Sources          Bronze Layer         Silver Layer         Gold Layer          Dashboard
─────────────         ────────────         ────────────         ──────────          ─────────
SAP ERP (CSV/day) ──► brz_sales_raw   ──► slv_sales       ──► gld_revenue      ──► Sales View
POS JSON (hourly) ──► brz_pos_raw     ──► slv_pos         ──► gld_inventory    ──► Inventory View
IoT Sensors (5min)──► brz_iot_raw     ──► slv_production  ──► gld_production   ──► Production View
Logistics (CSV/day)──► brz_logistics  ──► slv_distribution──► gld_distribution ──► Distribution View
                                      ──► slv_customers   ──► gld_customers    ──► Customer View
                                                                                ──► Genie NL Query
```

---

## 4. Key Metrics & KPIs (Gold Layer Targets)

### 4.1 Sales Domain

| KPI | Definition | Grain | Frequency | Source |
|---|---|---|---|---|
| **Daily Revenue** | Sum of net sales value (after returns, before distributor margins) | By product category × region × channel | Daily | POS + ERP |
| **Revenue vs. Prior Month** | % change in revenue vs. same period last month | By category and region | Daily | POS + ERP |
| **Revenue vs. Prior Year** | % change vs. same calendar period last year | Company-wide and by channel | Daily | POS + ERP |
| **Top SKUs by Revenue** | Ranked list of SKUs by net revenue | By region and channel | Weekly | POS + ERP |

**Revenue definition:** Net of returns; uses ERP invoice price (not shelf retail price); USD only; excludes trade spend/promotions (tracked separately).

### 4.2 Inventory Domain

| KPI | Definition | Grain | Frequency | Source |
|---|---|---|---|---|
| **Current Stock Level** | Units on hand per SKU per warehouse | By SKU × warehouse | Hourly (near-real-time) | ERP |
| **Inventory Turnover Rate** | COGS / Average Inventory Value over 30 days | By warehouse | Weekly | ERP |
| **Days Sales of Inventory (DSI)** | (Current Stock / 30-day avg daily sales) × 1 | By SKU and company-wide | Daily | ERP + POS |
| **Reorder Alert Flag** | Binary flag: current stock < reorder_point threshold | By SKU × warehouse | Hourly | ERP |

**Reorder point definition:** Configured per SKU in a reference table (`slv_freshsip.ref_reorder_points`); initially seeded from synthetic data, to be updated from SAP when live data available.

### 4.3 Production Domain

| KPI | Definition | Grain | Frequency | Source |
|---|---|---|---|---|
| **Batch Yield Rate** | (Actual output units / Expected output units) × 100 | Per batch | Per batch completion | IoT sensors |
| **Quality Check Pass Rate** | (Batches passed QC / Total batches) × 100 | By production line × product | Daily | IoT sensors |
| **Downtime Hours** | Total hours of unplanned production stoppage | By production line | Daily | IoT sensors |
| **Batch Traceability** | batch_id → shipment_id → retailer_id linkage | Per batch | On demand | ERP + IoT + Logistics |

**Yield rate definition:** Output measured in standard cases equivalent; "expected output" = batch recipe standard yield × raw material inputs.

### 4.4 Distribution Domain

| KPI | Definition | Grain | Frequency | Source |
|---|---|---|---|---|
| **On-Time Delivery %** | (Shipments delivered on or before promised date / Total shipments) × 100 | By channel × region | Daily | Logistics partner |
| **Order Fulfillment Rate** | (Orders fully shipped / Total orders) × 100 | By distribution channel | Daily | ERP + Logistics |
| **Cost Per Case Delivered** | Total logistics cost / Cases delivered | By region | Weekly | Logistics partner |
| **Worst-Performing Routes** | Routes ranked by OTD % ascending | Top 10 worst routes | Weekly | Logistics partner |

**On-time definition:** Receipt date at destination warehouse or retailer DC vs. promised delivery date in ERP order.

### 4.5 Customer Domain

| KPI | Definition | Grain | Frequency | Source |
|---|---|---|---|---|
| **Top 20 Retailers by Revenue** | Ranked list, revenue and % of total | Company-wide | Weekly | POS + ERP |
| **Customer Acquisition Cost (CAC)** | Total sales & marketing spend / New retailer accounts acquired | By retail segment | Monthly | ERP (spend) + CRM |
| **Retailer Retention Rate** | % of active retailers in prior period still active this period | By region | Monthly | ERP |
| **Revenue Concentration Risk** | % of total revenue from top 5 retailers | Company-wide | Monthly | POS + ERP |

**CAC definition:** For B2B CPG context, "customer" = retailer account; CAC = trade spend + broker commissions + field sales cost allocated to new account acquisition. Denominator = new accounts activated in the period.

---

## 5. Technical Constraints

| Constraint | Impact | Mitigation |
|---|---|---|
| **Databricks Community Edition (free tier)** | Single cluster; auto-terminates after idle; no SLAs; no Unity Catalog | Hive metastore fallback; scheduled pipeline windows; pre-warm before demo |
| **No streaming SLA on free tier** | True Spark Structured Streaming is unreliable | Micro-batch trigger every 5 minutes for IoT data; documented as "near-real-time (5-min refresh)" |
| **Team = CEO + AI agents only** | No DBA, data ops, or BI developer | AI agents must handle all code generation, review, and deployment |
| **4-week timeline** | Insufficient for all 5 domains at full depth | Phased delivery (see Section 7); Sales + Inventory are MVP |
| **No historical data at launch** | Cannot populate year-over-year comparisons | Synthetic data seeded with 13 months of history; swap for real data when available |
| **SAP/ERP data quality unknown** | Silver layer validation complexity is unforeseeable | Build aggressive DQ checks; fail fast on schema changes; log all rejected rows |
| **Free tier DBFS storage** | Limited total storage | Compact Delta tables; purge raw Bronze after 90 days; use small synthetic datasets |

### Data Freshness SLAs (by Domain)

| Domain | Target Refresh | Acceptable Maximum | Source Frequency |
|---|---|---|---|
| Sales (POS) | 1 hour | 2 hours | Hourly JSON from retailers |
| Inventory | 1 hour | 4 hours | Daily ERP + derived from sales |
| Production | 5 minutes (micro-batch) | 15 minutes | IoT streaming |
| Distribution | 4 hours | 12 hours | Daily logistics CSV |
| Customers | 24 hours | 48 hours | Daily ERP |

---

## 6. Risks

### Risk Register

| # | Risk | Probability | Impact | Severity | Mitigation |
|---|---|---|---|---|---|
| R1 | SAP CSV schema inconsistency / data quality issues slow Silver layer development | High | High | **Critical** | Build schema validation + row rejection in Bronze; mock clean data for dev; allocate buffer time |
| R2 | Scope creep — CEO adds requests during development | High | High | **Critical** | Strict MoSCoW enforcement; change requests go to backlog after sprint planning |
| R3 | Free tier cluster unavailable during board demo | Medium | Critical | **Critical** | Maintain separate pre-loaded demo environment; warm cluster 30 min before; have screenshot backup slides |
| R4 | 4-week timeline insufficient for all 5 domains | High | Medium | **High** | Phase delivery — Sales+Inventory MVP must be demo-ready by week 2; remaining domains are bonus |
| R5 | IoT streaming unstable on free tier | High | Medium | **High** | Use micro-batch pattern; never promise sub-minute latency |
| R6 | Synthetic data looks unrealistic — undermines board credibility | Medium | High | **High** | Generate realistic data with embedded narratives; CEO reviews data before board meeting |
| R7 | KPI definitions disputed post-build (e.g., which "revenue" number?) | Medium | High | **High** | Lock all KPI definitions in this brief before any code; CEO sign-off required |
| R8 | Unity Catalog unavailable — complicates catalog strategy | Medium | Low | **Medium** | Hive metastore fallback documented and ready; transparent to end users |
| R9 | PII / data governance concerns with retailer data | Low | Medium | **Low** | Use synthetic data only; if real data used, restrict workspace access to named users |
| R10 | Historical backfill gap — no year-over-year data at launch | High | Low | **Medium** | Seed 13 months of synthetic history; clearly label as "simulated" until real data backfill |

---

## 7. Phased Delivery Plan

### MVP Definition (Board Demo Minimum)
A working dashboard with Sales + Inventory domains, realistic synthetic data, and a live Databricks workspace link that loads in under 5 seconds. Production yield trend as a bonus.

### Phase Schedule

| Week | Deliverables | Domains | Success Signal |
|---|---|---|---|
| **Week 1** | Bronze + Silver pipelines for Sales (POS + ERP); Synthetic data seeded; Hive metastore schema created | Sales | Silver table `slv_sales` populated; row count + null check pass |
| **Week 2** | Gold layer for Sales KPIs; Inventory Bronze → Gold pipeline; Dashboard: Sales + Inventory pages live | Sales, Inventory | CEO can view daily revenue chart and inventory reorder alerts on dashboard |
| **Week 3** | Production IoT micro-batch pipeline; Distribution pipeline; Customer Gold layer | Production, Distribution, Customers | All 5 domain Gold tables populated with synthetic data |
| **Week 4** | Dashboard polish (all 5 domains); Genie Space (stretch); Demo environment prep; Board narrative | All + Polish | CEO completes a full demo walkthrough; board deck screenshot backup created |

---

## 8. MoSCoW Prioritization

### Must Have (MVP — board demo cannot proceed without these)
- Sales daily revenue by category, region, channel with trend
- Inventory current stock levels and reorder alerts
- Dashboard with at least 2 domain pages, loading under 5 seconds
- Realistic synthetic data covering 13 months
- At least 1 comparison view (vs. last month)

### Should Have (Week 3 target)
- Production yield rate and quality check pass rate
- Distribution on-time delivery % and fulfillment rate
- Customer top 20 retailers by revenue
- All 8 gold KPIs from project-context.md

### Could Have (Week 4 stretch)
- Genie AI/BI natural language query interface
- Automated "CEO Morning Brief" dashboard page (5 KPIs, mobile-friendly)
- Predictive stockout alert (days-of-supply below 7 days)
- Batch-to-shelf traceability view
- Anomaly detection flags on production yield

### Won't Have (Post-board; Phase 2)
- ML demand forecasting
- Real-time Kafka/Zerobus streaming ingestion
- Real SAP data integration (backfill + live)
- Role-based access control (different views per user)
- Automated board deck PDF generation
- Multi-tenant (other companies) support

---

## 9. Success Criteria

### Technical Acceptance Criteria
- All Gold tables populated with synthetic data passing DQ checks (null rate < 1%, no duplicate keys)
- Dashboard loads in ≤ 5 seconds on a pre-warmed free tier cluster
- Pipeline end-to-end (Bronze → Silver → Gold) completes in < 30 minutes for daily batch
- IoT micro-batch updates dashboard within 10 minutes of sensor data arrival
- All KPI calculations match definitions in Section 4 (verified by spot-check queries)

### Business Acceptance Criteria
- CEO can answer these 5 questions from the dashboard in < 2 minutes each:
  1. "What was total revenue last week vs. the week before?"
  2. "Which warehouse is closest to a stockout right now?"
  3. "What was the yield rate on the most recent production batch?"
  4. "Which distribution region has the worst on-time delivery?"
  5. "Who are my top 5 retailers by revenue this month?"
- Board presentation: at least 3 board members comment positively on the operational visibility
- Zero critical bugs (broken charts, wrong numbers) during the board demo

### Stretch Success Criteria
- Genie Space correctly answers at least 3 natural language CEO queries
- Dashboard shared link is accessible without Databricks login (published public view)

---

## 10. Open Questions (Require CEO Input)

| # | Question | Blocks |
|---|---|---|
| Q1 | Is "revenue" gross (invoice price) or net (after returns and trade spend)? | All Sales KPIs |
| Q2 | Is "on-time delivery" measured at ship date or receipt date vs. promised date? | OTD KPI |
| Q3 | For CAC — is the "customer" a retailer account or an end consumer? | Customer KPI |
| Q4 | Who else needs dashboard access besides the CEO? Any role-based filtering needed? | Access design |
| Q5 | What are the reorder point thresholds per SKU? Can we use industry defaults for synthetic data? | Inventory alerts |
| Q6 | How far back should the synthetic data history go? (Recommendation: 13 months for YoY) | Historical comparisons |
| Q7 | What alert delivery channel is preferred — dashboard-only, email, or Slack? | Alert design |
| Q8 | Is there an existing Databricks workspace, or does one need to be provisioned? | Infrastructure |

---

## Appendix A: Glossary

| Term | Definition |
|---|---|
| **DSI** | Days Sales of Inventory — (stock on hand / average daily sales) |
| **OTD** | On-Time Delivery — shipments arriving on or before promised date |
| **CAC** | Customer Acquisition Cost — total spend to acquire one new customer |
| **SKU** | Stock Keeping Unit — unique product identifier |
| **Batch** | A single production run of one product at one time |
| **Micro-batch** | Structured Streaming trigger pattern — processes data in small intervals (e.g., every 5 min) rather than continuous streaming |
| **Medallion** | Bronze (raw) → Silver (clean) → Gold (aggregated) data architecture pattern |
| **DQ** | Data Quality — validation checks for nulls, ranges, referential integrity |

---

*This document should be reviewed and approved by the CEO before the Data Architecture and BRD phases begin. Open questions in Section 10 must be resolved before sprint planning.*
