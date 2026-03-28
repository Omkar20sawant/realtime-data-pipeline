# DAY 9.2 — Dashboard Enhancements (Polish + UX)

## Overview
This update transforms the dashboard from a static data display into an interactive monitoring tool.  
The focus is on usability, real-time behavior, and interpretability.

---

## Enhancements Implemented

### 1. Auto-Refresh
- Dashboard refreshes automatically every 5 seconds  
- Simulates real-time monitoring without manual reload  
- Implemented using `streamlit-autorefresh`

---

### 2. Sidebar Time Window Selector
- Added interactive sidebar filter for selecting time window (in days)  
- Options: 1, 3, 7, 14 days  
- Drives all downstream queries  

---

### 3. Time-Based Filtering (Key Improvement)
Replaced row-based queries:

```sql
LIMIT 120
```

With:

```sql
WHERE window_start >= NOW() - INTERVAL 'N days'
```

This ensures:
- consistent time coverage  
- independence from data volume  
- alignment with real-world monitoring systems  

---

### 4. KPI Status Indicators
Added health classification for key metrics:

**Freshness Gap**
- <10 sec → Healthy  
- 10–30 sec → Warning  
- >30 sec → Critical  

**P95 Latency**
- <300 sec → Healthy  
- 300–900 sec → Warning  
- >900 sec → Critical  

Displayed directly in KPI cards.

---

### 5. Anomaly Alerts
Added dynamic alerts using:
- `st.error()`  
- `st.warning()`  
- `st.success()`  

Examples:
- Freshness lag → error/warning  
- High latency → warning/error  

Simulates production monitoring alerts.

---

### 6. Tab-Based Layout
Refactored dashboard into 3 tabs:

**Overview**
- KPI summary  
- status interpretation  

**Trends**
- events per minute chart  
- latency chart  

**Raw Data**
- recent metric records  

Improves readability and navigation.

---

## Result

The dashboard now behaves like a monitoring system:
- interactive  
- time-aware  
- self-explanatory  
- alert-driven  

---

## Key Takeaway

Transitioned from:
> data display dashboard  

To:
> real-time monitoring tool  
