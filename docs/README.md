# Documentation

Welcome to the Tiktok_OpenDB_server documentation!

## 📚 Available Guides

### [Facebook Field Configuration Guide](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md)

Complete guide for adding and configuring fields in Facebook Daily Reports.

**Topics Covered:**
- ✅ Architecture overview and design philosophy
- ✅ Adding object fields (adset, campaign, creative info)
- ✅ Adding insight fields (performance metrics)
- ✅ Adding conversion metrics (actions, video views)
- ✅ Configuration file structure
- ✅ Step-by-step examples
- ✅ Best practices and common pitfalls
- ✅ Troubleshooting guide

**Quick Links:**
- [Architecture Overview](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#architecture-overview)
- [Adding New Fields](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#adding-new-fields)
- [Examples](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#examples)
- [Troubleshooting](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#troubleshooting)

---

## 🚀 Quick Start

### For Developers

1. **Adding a simple metric?** → See [Insight Fields](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#2-insight-fields)
2. **Adding bid/budget info?** → See [Object Fields](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#1-object-fields)
3. **Adding conversion metric?** → See [Conversion Metrics](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#3-conversion-metrics)

### Common Tasks

| Task | Guide Section |
|------|---------------|
| Add new ad metadata | [Object Fields Example](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#example-1-add-optimization-goal-to-ad-fields) |
| Add performance metric | [Insight Fields](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#2-insight-fields) |
| Add conversion tracking | [Conversion Metrics Example](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#example-2-add-post-shares-metric) |
| Fix missing field | [Troubleshooting Issue 1](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#issue-1-field-not-showing-in-output) |
| Debug API errors | [Troubleshooting Issue 2](FACEBOOK_FIELD_CONFIGURATION_GUIDE.md#issue-2-field-not-found-error-from-api) |

---

## 📂 Project Structure

```
Tiktok_OpenDB_server/
├── docs/
│   ├── README.md (this file)
│   └── FACEBOOK_FIELD_CONFIGURATION_GUIDE.md
├── services/
│   └── facebook/
│       ├── constant.py          # Configuration & mapping
│       ├── base_processor.py    # Base API client
│       └── daily_processor.py   # Daily report logic
└── ...
```

---

## 🔧 Key Concepts

### Template-Driven Architecture

All field configurations are defined in templates (`constant.py`). Code automatically loads and processes based on these templates.

**Benefits:**
- ✅ Single source of truth
- ✅ No hardcoded field logic
- ✅ Easy to add/remove fields
- ✅ Consistent behavior

### Field Types

1. **Object Fields**: Metadata (id, name, status, bid info)
2. **Insight Fields**: Performance metrics (spend, impressions, clicks)
3. **Conversion Metrics**: Actions & conversions (leads, purchases, video views)

---

## 📖 Further Reading

- [Facebook Marketing API Documentation](https://developers.facebook.com/docs/marketing-api/)
- [Insights API Reference](https://developers.facebook.com/docs/marketing-api/insights/)

---

**Last Updated:** 2026-02-03
