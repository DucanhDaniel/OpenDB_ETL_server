
CONVERSION_METRICS_MAP = {

 "New Messaging Connections (N)": { 
   "api_field": "actions:onsite_conversion.messaging_first_reply",
   "parent_field": "actions",
 },
 "Cost per New Messaging (N)": { 
   "api_field": "cost_per_action_type:onsite_conversion.messaging_first_reply",
   "parent_field": "cost_per_action_type",
 },

 "New Messaging Connections": { 
   "api_field": "actions:onsite_conversion.messaging_conversation_started_7d",
   "parent_field": "actions",
 },
 "Cost per New Messaging": {
   "api_field": "cost_per_action_type:onsite_conversion.messaging_conversation_started_7d",
   "parent_field": "cost_per_action_type",
 },

 "Leads": {
   "api_field": "actions:lead",
   "parent_field": "actions",
 },
 "Cost Leads": {
   "api_field": "cost_per_action_type:lead",
   "parent_field": "cost_per_action_type",
 },
 "Purchases": {
   "api_field": "actions:omni_purchase",
   "parent_field": "actions",
 },
 "Cost Purchases": {
   "api_field": "cost_per_action_type:omni_purchase",
   "parent_field": "cost_per_action_type",
 },
 "Purchase Value": {
   "api_field": "action_values:omni_purchase",
   "parent_field": "action_values",
 },
 "Purchase ROAS": {
   "api_field": "purchase_roas",
   "parent_field": "purchase_roas",
 },
 "Website Purchases": {
   "api_field": "actions:offsite_conversion.fb_pixel_purchase",
   "parent_field": "actions",
 },
 "On-Facebook Purchases": {
   "api_field": "actions:onsite_conversion.purchase",
   "parent_field": "actions",
 },
  "Post comments": {
    "api_field": "actions:comment",
    "parent_field": "actions",
  },
  "Cost per post comment": {
    "api_field": "cost_per_action_type:comment",
    "parent_field": "cost_per_action_type",
  },
 "Link clicks": {
   "api_field": "actions:link_click",
   "parent_field": "actions",
 },
 "Cost per unique link click": {
   "api_field": "cost_per_unique_inline_link_click",
   "parent_field": "cost_per_unique_inline_link_click", 
 },
 "Leads Conversion Value": {
   "api_field": "action_values:lead",
   "parent_field": "action_values",
 },
 "Landing page views": {
   "api_field": "actions:landing_page_view",
   "parent_field": "actions",
 },
 "Cost per landing page view": {
   "api_field": "cost_per_action_type:landing_page_view",
   "parent_field": "cost_per_action_type",
 },
 "Adds to cart": {
   "api_field": "actions:omni_add_to_cart",
   "parent_field": "actions",
 },
 "Cost per add to cart": {
   "api_field": "cost_per_action_type:omni_add_to_cart",
   "parent_field": "cost_per_action_type",
 },
 "Checkouts Initiated": {
   "api_field": "actions:omni_initiated_checkout",
   "parent_field": "actions",
 },
 "Cost per checkout initiated": {
   "api_field": "cost_per_action_type:omni_initiated_checkout",
   "parent_field": "cost_per_action_type",
 }, 
  "Hoàn tất đăng ký": {
    "api_field": "actions:complete_registration",
    "parent_field": "actions",
  },
  "Chi phí / Hoàn tất đăng ký": {
    "api_field": "cost_per_action_type:complete_registration",
    "parent_field": "cost_per_action_type",
  },
  "ThruPlay": {
    "api_field": "actions:video_thruplay",
    "parent_field": "actions",
  },
  "Chi phí / ThruPlay": {
    "api_field": "cost_per_action_type:video_thruplay",
    "parent_field": "cost_per_action_type",
  },
  "Page Likes": {
    "api_field": "actions:like",
    "parent_field": "actions",
  },
  "Cost Page Likes": {
    "api_field": "cost_per_action_type:like",
    "parent_field": "cost_per_action_type",
  },

  "Page Follows": {
    "api_field": "actions:page_follow",
    "parent_field": "actions",
  },
  "Cost Page Follows": {
    "api_field": "cost_per_action_type:page_follow",
    "parent_field": "cost_per_action_type",
  },
  
"Video Views (3s)": {
    "api_field": "actions", 
    "parent_field": "actions", 
    "action_type": "video_view"
  },
  
  "Video Views (10s)": {
    "api_field": "actions",
    "parent_field": "actions",
    "action_type": "video_view_10s" 
  },

  "ThruPlay": {
    "api_field": "video_thruplay_watched_actions",
    "parent_field": "video_thruplay_watched_actions", 
    "action_type": "video_view"
  },
  "Video ThruPlay": {
    "api_field": "video_thruplay_watched_actions",
    "parent_field": "video_thruplay_watched_actions",
    "action_type": "video_view"
  },
  "Chi phí / ThruPlay": {
    "api_field": "cost_per_thruplay",
    "parent_field": "cost_per_thruplay",
    "action_type": "video_view"
  },
  "Video Views (30s)": {
    "api_field": "video_30_sec_watched_actions",
    "parent_field": "video_30_sec_watched_actions",
    "action_type": "video_view"
  },

  "Video Views (25%)": {
    "api_field": "video_p25_watched_actions",
    "parent_field": "video_p25_watched_actions",
    "action_type": "video_view"
  },
  "Video Views (50%)": {
    "api_field": "video_p50_watched_actions",
    "parent_field": "video_p50_watched_actions",
    "action_type": "video_view"
  },
  "Video Views (75%)": {
    "api_field": "video_p75_watched_actions",
    "parent_field": "video_p75_watched_actions",
    "action_type": "video_view"
  },
  "Video Views (95%)": {
    "api_field": "video_p95_watched_actions",
    "parent_field": "video_p95_watched_actions", 
    "action_type": "video_view"
  },
  "Video Views (100%)": {
    "api_field": "video_p100_watched_actions",
    "parent_field": "video_p100_watched_actions",
    "action_type": "video_view"
  },

  "Video Plays": {
    "api_field": "video_play_actions",
    "parent_field": "video_play_actions",
    "action_type": "video_view"
  },
  "Avg Time Watched": {
    "api_field": "video_avg_time_watched_actions",
    "parent_field": "video_avg_time_watched_actions",
    "action_type": "video_view"
  },
  "Messaging conversations started": { "api_field": "actions:onsite_conversion.messaging_conversation_started_7d", "parent_field": "actions", "action_type": "onsite_conversion.messaging_conversation_started_7d" },
  "New messaging contacts": { "api_field": "actions:onsite_conversion.messaging_first_reply", "parent_field": "actions", "action_type": "onsite_conversion.messaging_first_reply" },
  "Cost per messaging conversation started": { "api_field": "cost_per_action_type:onsite_conversion.messaging_conversation_started_7d", "parent_field": "cost_per_action_type", "action_type": "onsite_conversion.messaging_conversation_started_7d" },

  "Inline link clicks": { "api_field": "inline_link_clicks" },
  "Unique inline link clicks": { "api_field": "unique_inline_link_clicks" },

  "outbound_clicks": { 
    "api_field": "outbound_clicks", 
    "parent_field": "outbound_clicks", 
    "action_type": "outbound_click" 
  },
  "unique_outbound_clicks": { 
    "api_field": "unique_outbound_clicks", 
    "parent_field": "unique_outbound_clicks", 
    "action_type": "outbound_click" 
  },
  "outbound_click_ctr": { 
    "api_field": "outbound_clicks_ctr", 
    "parent_field": "outbound_clicks_ctr", 
    "action_type": "outbound_click" 
  },

  "Inline link CTR": { "api_field": "inline_link_click_ctr" },

  "Post saves": { "api_field": "actions:post_save", "parent_field": "actions", "action_type": "post_save" },
  "Post shares": { "api_field": "actions:post", "parent_field": "actions", "action_type": "post" },
  
  "Photo views": {
    "api_field": "actions:photo_view",
    "parent_field": "actions",
    "action_type": "photo_view"
  },
  
  "Post engagements": {
    "api_field": "actions:post_engagement",
    "parent_field": "actions",
    "action_type": "post_engagement"
  },
  "Post reactions": {
    "api_field": "actions:post_reaction",
    "parent_field": "actions",
    "action_type": "post_reaction"
  }
}

FACEBOOK_REPORT_TEMPLATES_STRUCTURE = [
 {
   "groupName": "(FAD) Facebook Ads Data",
   "templates": [
     {
       "name": "Campaign Overview Report",
       "config": {
         "type": "FAD_PERFORMANCE",
         "api_params": { "level": "campaign" },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "campaign_id",
             "campaign_name",
             "objective",
             "account_id",
             "account_name",
           ],
           "2.Trạng thái & Thời gian": [
             "status",
             "effective_status",
             "start_time",
             "stop_time",
             "created_time",
             "updated_time",
           ],
           "3.Cấu hình Đấu thầu": ["buying_type", "bid_strategy"],
           "4.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "cpc",
             "cpm",
             "ctr",
           ],
           "5.Khoảng thời gian tải dữ liệu": ["date_start", "date_stop"],
           "6.Chỉ số Hiệu suất & Chuyển đổi": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",     
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",          
             "Chi phí / ThruPlay"   
           ],
         },
         "campaign_fields": [
           "campaign_id", 
           "campaign_name", 
           "account_id",
           "status",
           "effective_status",
           "start_time",
           "stop_time",
           "created_time",
           "updated_time",
           "buying_type",
           "bid_strategy",
         ],
         "insight_fields": [ 
           "campaign_id", 
           "campaign_name", 
           "spend",
           "impressions",
           "reach",
           "clicks",
           "cpc",
           "cpm",
           "ctr",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
      {   
        "name": "Campaign Performance by AGE & GENDER",
        "config": {
          "type": "FAD_CAMPAIGN_BY_AGE", 
          "api_params": {
            "level": "campaign",
            "breakdowns": ["age", "gender"], 
            "time_increment": 1, 

          },
          "selectable_fields": {
            "1.Thông tin Chiến dịch": [
              "campaign_id",
              "campaign_name",
              "account_id",
              "account_name",
            ],
            "2.Phân loại (Breakdown)": ["age", "gender"], 
            "3.Chỉ số Hiệu suất & Chuyển đổi": [
              "spend",
              "New Messaging Connections",
              "Cost per New Messaging",
              "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
              "Leads",
              "Cost Leads",
              "Purchases",
              "Cost Purchases",
              "Purchase Value",
              "Purchase ROAS",
              "Website Purchases",
              "On-Facebook Purchases",
              "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
            ],
            "4.Khoảng thời gian tải dữ liệu": ["date_start", "date_stop"],
          },
          "campaign_fields": ["id", "name"],
          "insight_fields": [
            "spend",
            "actions",
            "action_values",
            "cost_per_action_type",
            "purchase_roas",
          ],
        },
      },
     {
       "name": "Campaign Performance by Age",
       "config": {
         "type": "FAD_CAMPAIGN_BY_AGE", 
         "api_params": {
           "level": "campaign",
           "breakdowns": "age", 
           "time_increment": 1, 
         },
         "selectable_fields": {
           "1.Thông tin Chiến dịch": [
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Phân loại (Breakdown)": ["age"],
           "3.Chỉ số Hiệu suất & Chuyển đổi": [
             "spend",
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "4.Khoảng thời gian tải dữ liệu": ["date_start", "date_stop"],
         },
         
         "campaign_fields": ["id", "name"],
         
         "insight_fields": [
           "spend",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
     {
       "name": "Campaign Performance by Gender",
       "config": {
         "type": "FAD_CAMPAIGN_BY_AGE",
          "api_params": {
           "level": "campaign",
           "breakdowns": "gender", 
           "time_increment": 1, 
         },
         "selectable_fields": {
           "1.Thông tin Chiến dịch": [
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Phân loại (Breakdown)": ["gender"],
           "3.Chỉ số Hiệu suất & Chuyển đổi": [
             "spend",
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "4.Khoảng thời gian tải dữ liệu": ["date_start", "date_stop"],
         },
         "campaign_fields": ["id", "name"],
         "insight_fields": [
           "spend",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       }
     },
     {
       "name": "Campaign Performance by Platform",
       "config": {
         "type": "FAD_CAMPAIGN_PLATFORM", 
         "api_params": {
           "level": "campaign",
           "breakdowns": ["publisher_platform", "platform_position"],
           "time_increment": 1,
         },
         "selectable_fields": {
           "1.Thông tin Định danh": [
             "account_id",
             "account_name",
             "campaign_id",
             "campaign_name",
           ],
           "2.Phân loại (Breakdown)": [
             "publisher_platform",
             "platform_position",
           ],
           "3.Thời gian": ["date_start", "date_stop"],
           "4.Chỉ số Hiệu suất": ["spend", "impressions", "clicks"],
           "5.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "insight_fields": [
           "account_id",
           "account_name",
           "campaign_id",
           "campaign_name",
           "spend",
           "impressions",
           "clicks",
           "date_start",
           "date_stop",
           "actions",
           "action_values",
         ],
       },
     },
     {
       "name": "Campaign Performance by Region",
       "config": {
         "type": "FAD_CAMPAIGN_BY_REGION",
         "api_params": {
           "level": "campaign",
           "breakdowns": "region",
         },
         "selectable_fields": {
           "1.Thông tin Chiến dịch": [
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Phân loại (Breakdown)": ["region"],
           "3.Chỉ số Hiệu suất": ["spend", "impressions", "clicks", "ctr"],
           "4.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "5.Khoảng thời gian lấy dữ liệu": ["date_start", "date_stop"],
         },
         "insight_fields": [
           "spend",
           "impressions",
           "clicks",
           "ctr",
           "actions",
           "action_values",
           "cost_per_action_type",
         ],
       },
     },
     {
       "name": "Campaign Performance by Hour (Audience Time)",
       "config": {
         "type": "FAD_CAMPAIGN_BY_HOUR", 
         "api_params": {
           "level": "campaign",
           "breakdowns": "hourly_stats_aggregated_by_audience_time_zone",
           "time_increment": 1, 
         },
         "selectable_fields": {
           "1.Thông tin Định danh": [
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Phân loại (Breakdown)": ["hour_of_day", "date_start", "date_stop"], 
           "3.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "clicks",
             "ctr",
             "cpc",
             "cpm"
           ],
           "4.Chỉ số Chuyển đổi": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "insight_fields": [
           "spend",
           "impressions",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "account_currency", 
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
     {
       "name": "Ad Set Performance Report",
       "config": {
         "type": "FAD_PERFORMANCE_ADSET",
         "api_params": { "level": "adset" },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "id",
             "name",
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Trạng thái & Chẩn đoán": [
             "status",
             "effective_status",
             "created_time",
           ],
           "3.Ngân sách": [
             "daily_budget",
             "lifetime_budget",
             "budget_remaining",
           ],
           "4.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "ctr",
             "cpc",
             "cpm",
           ],
           "5.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "Khoảng thời gian lấy dữ liệu": ["date_start", "date_stop"],
         },
         "adset_fields": [
           "id",
           "name",
           "campaign{name,id}",
           "status",
           "effective_status",
           "learning_stage_info",
           "issues_info",
           "daily_budget",
           "lifetime_budget",
           "budget_remaining",
           "created_time",
         ],
         "insight_fields": [
           "spend",
           "impressions",
           "reach",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
     {
       "name": "Ad Performance Report",
       "config": {
         "type": "FAD_PERFORMANCE_AD",
         "api_params": { "level": "ad" },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "id",
             "name",
             "adset_id",
             "adset_name",
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
             "created_time",
             "updated_time",
           ],
           "3.Trạng thái & Chẩn đoán": ["status", "effective_status"],
           "4.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "ctr",
             "cpc",
             "cpm",
             "frequency",
             "Post comments", 
             "Link clicks", 
             "Cost per unique link click", 
           ],
           "5.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Leads Conversion Value", 
             "Landing page views", 
             "Cost per landing page view", 
             "Adds to cart", 
             "Cost per add to cart", 
             "Checkouts Initiated", 
             "Cost per checkout initiated", 
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "6.Khoảng thời gian lấy dữ liệu": ["date_start", "date_stop"],
         },
         "ad_fields": [
           "id",
           "name",
           "adset{id,name,bid_strategy,bid_amount,daily_budget,lifetime_budget}",
           "campaign{name,id}",
           "status",
           "effective_status",
           "issues_info",
           "ad_review_feedback",
           "creative{instagram_permalink_url}",
           "created_time",
           "updated_time",
         ],
         "insight_fields": [
           "spend",
           "impressions",
           "reach",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "frequency",
           "actions",
           "action_values", 
           "cost_per_action_type",
           "purchase_roas",
           "cost_per_unique_inline_link_click", 
         ],
       },
     },
     {
       "name": "Account Daily Report",
       "config": {
         "type": "FAD_ACCOUNT_DAILY", 
         "api_params": {
           "level": "account",
           "time_increment": 1,
           "action_report_time": "conversion",
         }, 
         "selectable_fields": {
           "1.Thông tin định danh": [
             "account_id",
             "account_name",
             "account_currency",
             "date_start",
             "date_stop",
           ],
           "2.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "clicks",
             "cpc",
             "cpm",
             "ctr",
             "reach",
             "frequency",
             "Post comments",
             "Link clicks",
             "Cost per unique link click",
           ],
           "3.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Leads Conversion Value", 
             "Landing page views", 
             "Cost per landing page view", 
             "Adds to cart", 
             "Cost per add to cart", 
             "Checkouts Initiated", 
             "Cost per checkout initiated", 
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "insight_fields": [
           "id", "name", "account_currency",
           "spend",
           "impressions",
           "clicks",
           "cpc",
           "cpm",
           "ctr",
           "reach",
           "frequency",
           "date_start",
           "date_stop",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
           "cost_per_unique_inline_link_click",
         ],
       },
     },
     {
       "name": "Campaign Daily Report",
       "config": {
         "type": "FAD_CAMPAIGN_DAILY", 
         "api_params": {
           "level": "campaign",
           "time_increment": 1, 
           "action_report_time": "conversion", 
         },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
             "date_start",
             "date_stop",
           ],
           "2.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "cpc",
             "cpm",
             "ctr",
             "frequency",
             "Post comments", 
             "Link clicks", 
             "Cost per unique link click", 
           ],
           "3.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Leads Conversion Value", 
             "Landing page views", 
             "Cost per landing page view", 
             "Adds to cart", 
             "Cost per add to cart", 
             "Checkouts Initiated", 
             "Cost per checkout initiated", 
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "insight_fields": [
           "campaign_id",
           "campaign_name",
           "account_id",
           "account_name",
           "spend",
           "impressions",
           "reach",
           "clicks",
           "cpc",
           "cpm",
           "ctr",
           "frequency",
           "date_start",
           "date_stop",
           "actions", 
           "action_values", 
           "cost_per_action_type", 
           "purchase_roas",
           "cost_per_unique_inline_link_click", 
         ],
       },
     },
     {
       "name": "Ad Set Daily Report",
       "config": {
         "type": "FAD_ADSET_DAILY",
         "api_params": {
           "level": "adset",
           "time_increment": 1,
           "action_report_time": "conversion",
         },
         "selectable_fields": {
           "Thông tin định danh": [
             "id",
             "name",
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
             "date_start",
             "date_stop",
           ],
           "2.Trạng thái & Chẩn đoán": ["status", "effective_status"],
           "3.Ngân sách": [
             "daily_budget",
             "lifetime_budget",
             "budget_remaining",
           ],
           "4.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "ctr",
             "cpc",
             "cpm",
             "frequency",
           ],
           "5.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
             "On-Facebook Purchases",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "adset_fields": [
           "id",
           "name",
           "campaign{name,id}",
           "status",
           "effective_status",
           "daily_budget",
           "lifetime_budget",
           "budget_remaining",
         ],
         "insight_fields": [
           "spend",
           "impressions",
           "reach",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "frequency",
           "date_start",
           "date_stop",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
     {
       "name": "Ad Daily Report",
       "config": {
         "type": "FAD_AD_DAILY",
         "api_params": {
           "level": "ad",
           "time_increment": 1,
           "action_report_time": "conversion",
         },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "id",
             "name",
             "adset_id",
             "adset_name",
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
             "date_start",
             "date_stop",
           ],
           "2.Trạng thái & Thời gian": [
             "status",
             "effective_status",
             "created_time",
           ],
           "3.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "reach",
             "clicks",
             "ctr",
             "cpc",
             "cpm",
             "frequency",
             "Post comments", 
             "Link clicks", 
             "Cost per unique link click", 
           ],
           "4.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Leads",
             "Cost Leads",
             "Leads Conversion Value", 
             "Landing page views", 
             "Cost per landing page view", 
             "Adds to cart", 
             "Cost per add to cart", 
             "Checkouts Initiated", 
             "Cost per checkout initiated", 
             "Purchases",
             "Cost Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Website Purchases",
              "On-Facebook Purchases",
              "Page Likes",         
              "Cost Page Likes",     
              "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
         },
         "ad_fields": [
           "id",
           "name",
           "adset{id,name,bid_strategy,bid_amount,daily_budget,lifetime_budget}",
           "campaign{name,id}",
           "status",
           "effective_status",
           "created_time",
         ],
         "insight_fields": [
           "spend",
           "impressions",
           "reach",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "frequency",
           "date_start",
           "date_stop",
           "actions", 
           "action_values", 
           "cost_per_action_type", 
           "purchase_roas",
           "cost_per_unique_inline_link_click", 
         ],
       },
     },
     {
       "name": "Ad Creative Report",
       "config": {
         "type": "FAD_AD_CREATIVE",
         "api_params": {
          "level": "ad"
          },
         "selectable_fields": {
           "1.Thông tin định danh": [
             "id",
             "name",
             "adset_id",
             "adset_name",
             "campaign_id",
             "campaign_name",
             "account_id",
             "account_name",
           ],
           "2.Trạng thái": ["status", "effective_status"],
           "3.Thông tin Sáng tạo": [
             "creative_id",
             "actor_id",
             "page_name",
             "creative_title",
             "creative_body",
             "creative_thumbnail_url",
             "creative_thumbnail_raw_url",
             "creative_link",
           ],
           "4.Chỉ số Hiệu suất": [
             "spend",
             "impressions",
             "Leads",
             "Cost Leads",
             "reach",
             "clicks",
             "ctr",
             "cpc",
             "cpm",
           ],
           "5.Chỉ số Chuyển đổi (Conversion)": [
             "New Messaging Connections",
             "Cost per New Messaging",
             "New Messaging Connections (N)", 
             "Cost per New Messaging (N)",
             "Purchases",
             "Purchase Value",
             "Purchase ROAS",
             "Hoàn tất đăng ký",      
             "Chi phí / Hoàn tất đăng ký", 
             "ThruPlay",              
             "Chi phí / ThruPlay"     
           ],
           "6.Khoảng thời gian lấy dữ liệu": ["date_start", "date_stop"],
         },
         "ad_fields": [
           "id",
           "name",
           "status",
           "effective_status",
           "adset{id,name,bid_strategy,bid_amount,daily_budget,lifetime_budget}",
           "campaign{id,name}",
           "creative{id,name,object_story_id,title,body,thumbnail_url,actor_id}",
         ],
         "insight_fields": [
           "spend",
           "impressions",
           "Leads",
           "Cost Leads",
           "reach",
           "clicks",
           "ctr",
           "cpc",
           "cpm",
           "actions",
           "action_values",
           "cost_per_action_type",
           "purchase_roas",
         ],
       },
     },
      
      {
        "name": "Ad Creative Daily Report",
        "config": {
          "type": "FAD_AD_CREATIVE_DAILY",
          # Dùng để init req url
          "api_params": { "level": "ad", "time_increment": 1 },
          # Dùng để hiển thị trên UI
          "selectable_fields": {
            "1.Thông tin định danh": ["id", "name", "adset_id", "adset_name", "campaign_id", "campaign_name", "account_id", "account_name", "date_start", "date_stop"],
            "2.Trạng thái": ["status", "effective_status"],
            "3.Thông tin Sáng tạo": [
                "creative_id", "actor_id", "page_name", "creative_title", "creative_body", 
                "creative_thumbnail_url", "creative_thumbnail_raw_url", "creative_link"
            ],
            "4.Chỉ số Hiệu suất": ["spend", "impressions", "reach", "clicks", "ctr", "cpc", "cpm", "frequency"],
            "5.Chỉ số Tương tác": ["Post engagement", "Post reactions", "Post comments", "Link clicks"],
            
            "6.Chỉ số Chuyển đổi": [
                "New Messaging Connections", "Cost per New Messaging",
                "New Messaging Connections (N)", "Cost per New Messaging (N)",
                "Leads", "Cost Leads",
                "Purchases", "Cost Purchases", 
                "Purchase Value", "Purchase ROAS",
                "Hoàn tất đăng ký", "Chi phí / Hoàn tất đăng ký", 
                "ThruPlay", "Chi phí / ThruPlay"
            ],
            "7.Video Views Detail": ["Video Views (3s)", "Video Views (30s)", "Video Views (25%)", "Video Views (50%)", "Video Views (75%)", "Video Views (100%)", "Avg Time Watched", "Video Plays"]
          },
          # Dùng để init request URL
          "ad_fields": ["id", "name", "status", "effective_status", "adset{id,name,bid_strategy,bid_amount,daily_budget,lifetime_budget}", "campaign{id,name}", "creative{id,name,object_story_id,title,body,thumbnail_url,actor_id}"],
          "insight_fields": [ 
              "spend", "impressions", "reach", "clicks", "ctr", "cpc", "cpm", "frequency", 
              "actions", "action_values", "cost_per_action_type", "purchase_roas", 
              "video_30_sec_watched_actions", "video_thruplay_watched_actions", "cost_per_thruplay", 
              "video_p25_watched_actions", "video_p50_watched_actions", "video_p75_watched_actions", "video_p95_watched_actions", "video_p100_watched_actions", "video_avg_time_watched_actions", "video_play_actions", 
              "date_start", "date_stop"
          ]
        }
      },
      
      {
        "name": "AGE & GENDER_DETAILED_REPORT",
        "config": {
          "type": "FAD_AD_CREATIVE_DAILY", 
          "api_params": { 
            "level": "ad", 
            "time_increment": 1, 
            "breakdowns": ["age", "gender"]
          },
          "selectable_fields": {
            "1. Thời gian & Tài khoản": ["date_start", "date_stop", "account_id", "account_name"],
            "2. Thông tin Quảng cáo": [
                "campaign_name", "adset_name", "name", "id", 
                "adset_bid_strategy", "adset_bid_amount",
                "age", "gender" 
            ],
            "3. Thông tin Sáng tạo (Creative)": [
                "creative_id", "creative_name", "creative_thumbnail_url", "creative_link"
            ],
            "4. Chi phí & Hiệu suất": [
                "spend", "impressions", "reach", "clicks", "cpc", "cpm", "ctr",
                "inline_link_clicks", "outbound_clicks", 
                "inline_link_click_ctr", "outbound_click_ctr"
            ],
            "5. Messaging & Tương tác": [
                "Messaging conversations started", "New messaging contacts", "Cost per messaging conversation started",
                "Post engagements", "Post reactions", "Post comments", "Post saves", "Post shares", "Photo views"
            ],
            "6. Video & Landing Page": [
                "Landing page views", "Cost per landing page view",
                "ThruPlay", "Chi phí / ThruPlay", 
                "Video Views (25%)", "Video Views (50%)", "Video Views (75%)", "Video Views (95%)", "Video Views (100%)"
            ]
          },
          "ad_fields": [
            "id", "name", "status", "effective_status", 
            "adset{id,name,bid_strategy,daily_budget,lifetime_budget,bid_amount}", 
            "campaign{id,name}",
            "creative{id,name,thumbnail_url,image_url,actor_id,object_story_id,title,body}" 
          ],
          "insight_fields": [
            "spend", "impressions", "reach", "clicks", "cpc", "cpm", "ctr", "frequency", "date_start", "date_stop",
            "inline_link_clicks", "unique_inline_link_clicks", "outbound_clicks", "unique_outbound_clicks",
            "inline_link_click_ctr", "outbound_clicks_ctr",
            "actions", "action_values", "cost_per_action_type",
            "video_play_actions", "video_thruplay_watched_actions",   
            "video_p25_watched_actions", "video_p50_watched_actions", "video_p75_watched_actions", 
            "video_p95_watched_actions", "video_p100_watched_actions"
          ]
        }
      },
      
      {
        "name": "LOCATION_DETAILED_REPORT",
        "config": {
          "type": "FAD_AD_CREATIVE_DAILY",
          "api_params": { 
            "level": "ad", 
            "time_increment": 1, 
            "breakdowns": ["country", "region"] 
          },
          "selectable_fields": {
            "1. Thời gian & Tài khoản": ["date_start", "date_stop", "account_id", "account_name"],
            "2. Thông tin Quảng cáo & Vị trí": [
                "campaign_name", "adset_name", "ad_name", "id", 
                "adset_bid_strategy", "adset_bid_amount",
                "country", "region" 
            ],
            "3. Thông tin Sáng tạo (Creative)": [
                "creative_id", "creative_name", "creative_thumbnail_url"
            ],
            "4. Chi phí & Hiệu suất": [
                "spend", "impressions", "reach", "clicks", "cpc", "cpm", "ctr", "frequency",
                "inline_link_clicks", "outbound_clicks"
            ],
            "5. Messaging & Tương tác": [
                "Messaging conversations started", "New messaging contacts", "Cost per messaging conversation started",
                "Post engagements", "Post reactions", "Post comments", "Post saves", "Post shares"
            ],
            "6. Video & Landing Page": [
                "Landing page views", "Cost per landing page view",
                "Video Plays", "ThruPlays", "Photo views"
            ]
          },
          "ad_fields": [
            "id", "name", "status", 
            "adset{id,name,bid_strategy,daily_budget,lifetime_budget}", 
            "campaign{id,name}",
            "creative{id,name,thumbnail_url,image_url}"
          ],
          "insight_fields": [
            "spend", "impressions", "reach", "clicks", "cpc", "cpm", "ctr", "frequency", "date_start", "date_stop",
            "inline_link_clicks", "outbound_clicks", "inline_link_click_ctr", "outbound_clicks_ctr",
            "actions", "action_values", "cost_per_action_type",
            "video_play_actions", "video_thruplay_watched_actions"
          ]
        }
      },
   ]
}
]

EFFECTIVE_STATUS_FILTERS = {
  "adset": ["ACTIVE", "PAUSED", "CAMPAIGN_PAUSED", "ARCHIVED", "IN_PROCESS", "WITH_ISSUES"],
  "ad": [
    "ACTIVE",
    "PAUSED",
    "ADSET_PAUSED",
    "CAMPAIGN_PAUSED",
    "WITH_ISSUES",
    "PENDING_REVIEW",
    "DISAPPROVED",
    "PREAPPROVED",
    "IN_PROCESS",
    "PENDING_BILLING_INFO",
    "ARCHIVED",
    # "DELETED"
    ]
}