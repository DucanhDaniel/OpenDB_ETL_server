# Marketing ETL Pipeline

This repository contains the backend server for the Marketing ETL Pipeline project, providing a reporting API, dashboard for monitoring, and asynchronous job processing capabilities.

## 🚀 Features

- **Dashboard**: A comprehensive dashboard to monitor tasks, logs, and system status (`/dashboard`).
- **Async Job Processing**: Utilizes **Celery** and **Redis** for handling background tasks such as generating reports and data processing.
- **Facebook Ads Integration**: Modules to process and fetch data from Facebook Ads (`services/facebook`).
- **Google Sheets Integration**: Functionality to write formatted data directly to Google Sheets (`services/sheet_writer`).
- **Currency Exchange**: Service for handling currency conversion (`services/currency`).
- **GMV Tracking**:  Tracking and analysis of Gross Merchandise Value (`services/gmv`).
- **API Documentation**: Auto-generated Swagger UI documentation available at `/docs`.

## 🛠 Tech Stack

- **Framework**: [FastAPI](https://fastapi.tiangolo.com/)
- **Database**: [MongoDB](https://www.mongodb.com/)
- **Message Broker**: [Redis](https://redis.io/)
- **Task Queue**: [Celery](https://docs.celeryq.dev/)
- **Containerization**: [Docker](https://www.docker.com/) & Docker Compose

## 📦 Installation & Setup

### Prerequisites

- Docker
- Docker Compose

### Steps

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd Marketing_ETL_Pipeline
    ```

2.  **Environment Configuration:**
    Create a `.env` file in the root directory. You can copy the structure from a sample if available, or ensure the following variables are set:
    *   `MONGO_ROOT_USER`
    *   `MONGO_ROOT_PASSWORD`
    *   `MONGO_URI="mongodb://${MONGO_ROOT_USER}:${MONGO_ROOT_PASSWORD}@mongo:27017/"`
    *   `MONGO_DATABASE`
    *   `REDIS_PASSWORD`
    *   `REDIS_HOST`
    *   `GOOGLE_CREDENTIALS_PATH`

3. **Running with Docker Compose**

    ```bash
    docker-compose up -d --build
    ```

4.  **Access the Application:**
    -   **API**: `http://localhost:8011`
    -   **Dashboard**: `http://localhost:8011/dashboard`
    -   **API Docs**: `http://localhost:8011/docs`
### Google Sheets Integration Setup

To enable the application to write reports to Google Sheets, follow these steps:

1.  **Create a Google Service Account:**
    *   Go to the [Google Cloud Console](https://console.cloud.google.com/).
    *   Create a new project or select an existing one.
    *   Navigate to **APIs & Services** > **Credentials**.
    *   Click **Create Credentials** > **Service Account**.
    *   Follow the prompts to create the account.
    *   Once created, go to the **Keys** tab of the service account and create a new key (JSON format).
    *   Download the JSON file.

2.  **Configure the Application:**
    *   Rename the downloaded JSON file to `credentials.json` and place it in the root directory of the project.
    *   **OR** set the `GOOGLE_CREDENTIALS_PATH` environment variable in your `.env` file to point to the absolute path of your JSON key file.

3.  **Share Access to Target Sheets:**
    *   Open the JSON key file you downloaded and find the `client_email` field (e.g., `service-account-name@project-id.iam.gserviceaccount.com`).
    *   Open the Google Sheet where you want reports to be written.
    *   Click the **Share** button.
    *   Paste the `client_email` address and give it **Editor** access.
    *   **Note:** The application will fail to write data if the service account does not have edit access to the destination sheet.



## 📖 API Documentation

The server exposes the following endpoints:

### 1. Create Report Job
*   **Endpoint**: `POST /reports/create-job`
*   **Description**: Queues a background job to generate a report.
*   **Body** (`application/json`):
    ```json
    {
      "task_type": "string (e.g., 'facebook_daily', 'tiktok_product')",
      "job_id": "string (unique identifier)",
      "task_id": "string",
      "access_token": "string (platform access token)",
      "start_date": "YYYY-MM-DD",
      "end_date": "YYYY-MM-DD",
      "user_email": "string",
      "spreadsheet_id": "string (Google Sheet ID)",
      "sheet_name": "string",
      "is_overwrite": false,
      "accounts": ["string (optional)"],
      "advertiser_id": "string (optional)",
      "store_id": "string (optional)"
    }
    ```

### 2. Dashboard Data
*   **Endpoint**: `GET /api/dashboard`
*   **Description**: Retrieves aggregated stats and a list of recent jobs for the dashboard.

### 3. Job Logs
*   **Endpoint**: `GET /api/dashboard/logs/{job_id}`
*   **Description**: Fetches the full execution logs for a specific job ID.

### 4. Cancel Job
*   **Endpoint**: `POST /reports/{job_id}/cancel`
*   **Description**: Requests cancellation of a running job.

For interactive documentation, visit `/docs` when the server is running.

## 📋 Supported Task Types & Templates

The system supports two main platforms: **Facebook Ads** and **TikTok GMV**.

### 1. Facebook Ads
*   **Required Params**: `access_token`, `accounts` (list of `{id, name}`), `start_date`, `end_date`, `template_name`, `spreadsheet_id`, `sheet_name`, `selected_fields`.

> [!NOTE]
> To find the full list of available `selected_fields` (or `selectable_fields` config) for each template, please refer to the constant files in the source code:
> *   **Facebook Ads**: Check `services/facebook/constant.py`.
> *   **TikTok GMV**: Check the reporter files in `services/gmv/`.

#### Task Type: `facebook_daily`
For reports with daily breakdown.

| Template Name | Description | Level |
| :--- | :--- | :--- |
| `Account Daily Report` | Daily breakdown at Account level | Account |
| `Campaign Daily Report` | Daily breakdown at Campaign level | Campaign |
| `Ad Set Daily Report` | Daily breakdown at Ad Set level | AdSet |
| `Ad Daily Report` | Daily breakdown at Ad level | Ad |
| `Ad Creative Daily Report` | Daily breakdown with Creative details | Ad |

#### Task Type: `facebook_performance`
For reports aggregated over the entire period.

| Template Name | Description | Level |
| :--- | :--- | :--- |
| `Campaign Overview Report` | General campaign performance | Campaign |
| `Ad Set Performance Report` | Ad Set level performance | AdSet |
| `Ad Performance Report` | Ad level performance | Ad |
| `Ad Creative Report` | Creative details and performance | Ad |

#### Task Type: `facebook_breakdown`
For reports broken down by dimensions like Age, Gender, Region, etc.

| Template Name | Description | Level |
| :--- | :--- | :--- |
| `Campaign Performance by AGE & GENDER` | Breakdown by Age and Gender | Campaign |
| `Campaign Performance by Age` | Breakdown by Age only | Campaign |
| `Campaign Performance by Gender` | Breakdown by Gender only | Campaign |
| `Campaign Performance by Platform` | Breakdown by Publisher Platform & Position | Campaign |
| `Campaign Performance by Region` | Breakdown by Region | Campaign |
| `Campaign Performance by Hour (Audience Time)` | Hourly breakdown | Campaign |
| `AGE & GENDER_DETAILED_REPORT` | Detailed Daily Ad level by Age & Gender | Ad |
| `LOCATION_DETAILED_REPORT` | Detailed Daily Ad level by Country & Region | Ad |

### 2. TikTok GMV
*   **Task Types**:
    *   `product`: Product performance detail.
    *   `creative`: Creative performance detail.
*   **Required Params**: `access_token`, `advertiser_id`, `store_id`, `start_date`, `end_date`, `spreadsheet_id`, `sheet_name`.

## 🔄 Usage Workflow

1.  **Prepare your Google Sheet**: Share edit access to the service account email.
2.  **Call API**: Send a `POST` request to `/reports/create-job`.

**Example Request (Facebook):**
```json
{
  "task_type": "facebook_daily",
  "job_id": "job_fb_001",
  "task_id": "task_fb_001",
  "access_token": "YOUR_FB_ACCESS_TOKEN",
  "start_date": "2023-10-01",
  "end_date": "2023-10-07",
  "user_email": "user@example.com",
  "spreadsheet_id": "1A2B3C...",
  "sheet_name": "FB_Report",
  "template_name": "Campaign Overview Report",
  "accounts": [
    {"id": "act_123456789", "name": "My Ad Account"}
  ],
  "selected_fields": [
    "campaign_name",
    "spend",
    "impressions",
    "clicks",
    "ctr",
    "cpc",
    "Purchases",
    "Cost Purchases",
    "Purchase ROAS"
  ]
}
```

**Example Request (TikTok GMV):**
```json
{
  "task_type": "product",
  "job_id": "job_tt_001",
  "task_id": "task_tt_001",
  "access_token": "YOUR_TIKTOK_ACCESS_TOKEN",
  "start_date": "2023-10-01",
  "end_date": "2023-10-07",
  "user_email": "user@example.com",
  "spreadsheet_id": "1A2B3C...",
  "sheet_name": "TikTok_Product_Report",
  "advertiser_id": "123456789",
  "store_id": "987654321"
}
```

## 🔒 Security & Nginx

The application uses **Nginx** as a reverse proxy and for basic authentication to protect sensitive endpoints.

### Protected Endpoints
The following endpoints are protected by HTTP Basic Auth:
*   `/dashboard`: The main dashboard UI.
*   `/api/dashboard`: API endpoints used by the dashboard.

### Resetting Dashboard Password
The credentials are stored in the `.htpasswd` file. To reset or change the password:

1.  **Install `htpasswd` utility** (if not available):
    *   **Ubuntu/Debian**: `sudo apt install apache2-utils`
    *   **Windows**: Use an online generator or Python script.

2.  **Generate new `.htpasswd` file**:
    ```bash
    # Usage: htpasswd -c .htpasswd <username>
    htpasswd -c .htpasswd admin
    # You will be prompted to enter the new password.
    ```

3.  **Restart Nginx container**:
    ```bash
    docker-compose restart nginx
    ```

## 📊 Dashboard Guide

The dashboard provides a centralized view of all reporting jobs.

### 1. Job List
The main view provides a summary of all active and completed jobs. You can monitor the status, start time, and progress of each report generation task.

![Job List Overview](docs/images/Screenshot%202026-02-15%20160725.png)
*Figure 1: Dashboard Overview*

![Job List Details](docs/images/Screenshot%202026-02-15%20160740.png)
*Figure 2: Viewing Top user status*

![Job List Pagination](docs/images/Screenshot%202026-02-15%20160755.png)
*Figure 3: Job List*

### 2. Task Details
Clicking on a specific **Job ID** opens the detailed view. Here you can see:
*   **API Usage Charts**: Visualize the number of API calls and potential rate limits.
*   **Execution Logs**: Real-time logs from the worker process.

**API Usage Tracking:**
![API Usage Charts](docs/images/Screenshot%202026-02-15%20161017.png)

**Worker Execution Logs:**
![Worker Logs](docs/images/Screenshot%202026-02-15%20161038.png)

## 🗂 Project Structure

- `main.py`: Entry point for the FastAPI application.
- `workers/`: Contains Celery task definitions and workers.
- `services/`: Business logic and service modules (Facebook, Dashboard, GMO, etc.).
- `models/`: Pydantic models and database schemas.
- `static/`: Static files for the dashboard (HTML, JS, CSS).
- `docker-compose.yml`: Docker services configuration.

## 🤝 Contributing

1.  Fork the repository.
2.  Create a new branch (`git checkout -b feature/your-feature`).
3.  Commit your changes (`git commit -am 'Add new feature'`).
4.  Push to the branch (`git push origin feature/your-feature`).
5.  Create a new Pull Request.
