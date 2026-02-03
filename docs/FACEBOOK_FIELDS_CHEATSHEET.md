# Facebook Fields Quick Reference

One-page cheat sheet for adding fields to Facebook Daily Reports.

---

## 🎯 Quick Decision Tree

```
Need to add a field?
    ↓
┌───────────────────────────────────────┐
│ What type of field is it?            │
└───────────────────────────────────────┘
    ↓
    ├─ Object info (id, name, status) → [Type 1: Object Fields](#type-1-object-fields)
    ├─ Performance metric (scalar)     → [Type 2: Insight Fields](#type-2-insight-fields)
    └─ Conversion/Action metric        → [Type 3: Conversion Metrics](#type-3-conversion-metrics)
```

---

## Type 1: Object Fields

**When:** Adding metadata (bid, budget, status, creative info)

**File:** `constant.py` → Template → `ad_fields`

**Template:**
```python
"ad_fields": [
  "adset{id,name,NEW_FIELD_HERE}"
]
```

**Response Processing:** `daily_processor.py` → `_process_nested_level_response()`
```python
if item.get("adset"):
    parent_info["adset_NEW_FIELD"] = item["adset"].get("NEW_FIELD")
```

---

## Type 2: Insight Fields

**When:** Adding simple performance metric

**File:** `constant.py` → Template → `insight_fields` + `selectable_fields`

**Template:**
```python
"insight_fields": ["spend", "NEW_FIELD"],
"selectable_fields": {
  "Group Name": ["spend", "NEW_FIELD"]
}
```

✅ **No code change needed!**

---

## Type 3: Conversion Metrics

**When:** Metric needs mapping (actions, video, conversions)

**Step 1:** `constant.py` → `CONVERSION_METRICS_MAP`
```python
"Friendly Name": {
  "api_field": "parent_field:action_type",  # or just "field_name"
  "parent_field": "parent_field",
  "action_type": "action_type"  # optional
}
```

**Step 2:** Add parent to `insight_fields`
```python
"insight_fields": ["actions", "parent_field"]
```

**Step 3:** Add to `selectable_fields`
```python
"selectable_fields": {
  "Group": ["Friendly Name"]
}
```

---

## 📋 Common Patterns

### Pattern 1: Actions-Based
```python
"Metric Name": {
  "api_field": "actions:ACTION_TYPE",
  "parent_field": "actions"
}
```
**Example:** `"api_field": "actions:lead"`

### Pattern 2: Video Metrics
```python
"Video Metric": {
  "api_field": "video_METRIC_watched_actions",
  "parent_field": "video_METRIC_watched_actions",
  "action_type": "video_view"
}
```
**Example:** `"api_field": "video_p50_watched_actions"`

### Pattern 3: Cost Metrics
```python
"Cost per X": {
  "api_field": "cost_per_action_type:ACTION_TYPE",
  "parent_field": "cost_per_action_type"
}
```
**Example:** `"api_field": "cost_per_action_type:lead"`

### Pattern 4: Scalar Value
```python
"Simple Metric": {
  "api_field": "field_name"
}
```
**Example:** `"api_field": "inline_link_clicks"`

---

## 🔍 Field Locations

### In Template Config:

| Section | Purpose | Example |
|---------|---------|---------|
| `api_params` | API behavior | `"level": "ad"` |
| `ad_fields` | Object structure | `"adset{id,name}"` |
| `insight_fields` | Performance fields | `"spend", "impressions"` |
| `selectable_fields` | UI display | `"1. Metrics": ["spend"]` |

### In Code:

| Function | Purpose |
|----------|---------|
| `_create_nested_level_url()` | Builds API request |
| `_process_nested_level_response()` | Extracts object data |
| `_flatten_action_metrics()` | Extracts metrics |

---

## ⚡ Common Fields Reference

### Object Fields
```python
# Adset
"adset{id,name,bid_strategy,bid_amount,optimization_goal,daily_budget,lifetime_budget}"

# Campaign
"campaign{id,name,objective,buying_type,status}"

# Creative
"creative{id,name,thumbnail_url,title,body,video_id,actor_id,object_story_id}"
```

### Insight Fields
```python
# Basic
"spend", "impressions", "reach", "clicks", "ctr", "cpc", "cpm", "frequency"

# Engagement
"inline_link_clicks", "outbound_clicks", "unique_inline_link_clicks"

# Action Parents
"actions", "action_values", "cost_per_action_type"

# Video Parents
"video_play_actions", "video_thruplay_watched_actions",
"video_p25_watched_actions", "video_p50_watched_actions",
"video_p75_watched_actions", "video_p100_watched_actions"
```

### Conversion Metrics (action_types)
```
lead
omni_purchase
onsite_conversion.messaging_conversation_started_7d
onsite_conversion.messaging_first_reply
landing_page_view
app_install
post_engagement
post_reaction
comment
like
video_view
```

---

## 🐛 Quick Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Field missing in output | Not in template config | Add to `ad_fields` or `insight_fields` |
| API 400 "field not found" | Typo or wrong level | Check spelling, verify field exists for level |
| Value always 0/None | Wrong action_type | Check CONVERSION_METRICS_MAP action_type |
| Field duplicated | In template + hardcoded | Remove hardcoded logic, use template only |

---

## 📝 Checklist for Adding New Field

### Object Field:
- [ ] Added to `ad_fields` with nested syntax
- [ ] Updated `_process_nested_level_response()` if nested
- [ ] Tested with 1-day date range

### Insight Field:
- [ ] Added to `insight_fields`
- [ ] Added to `selectable_fields`
- [ ] Tested with 1-day date range

### Conversion Metric:
- [ ] Added to `CONVERSION_METRICS_MAP`
- [ ] Parent field in `insight_fields`
- [ ] Added to `selectable_fields`
- [ ] Verified action_type correct
- [ ] Tested with 1-day date range

---

## 🔗 Full Documentation

For detailed explanations and examples, see [FACEBOOK_FIELD_CONFIGURATION_GUIDE.md](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md)

---

**Last Updated:** 2026-02-03
