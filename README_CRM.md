# Lead Enrichment CRM

A comprehensive CRM web interface for managing your lead enrichment pipeline, built with FastAPI and modern web technologies.

## Features

### Dashboard
- Real-time pipeline statistics and progress tracking
- Budget monitoring with provider-level cost breakdown
- Lead status and tier distribution
- Recent leads overview
- Quick action buttons for pipeline operations

### Leads Management
- Comprehensive leads list with filtering by status, tier, and score
- Detailed lead profiles showing:
  - Company information (firmographics, funding, location)
  - Contact details with email verification status
  - AI-generated research summaries
  - Personalized icebreakers
  - Email templates and variants
  - Score breakdown and trigger events
  - Campaign performance metrics
- Export leads to JSON
- Direct links to LinkedIn and company websites

### Campaign Management
- Create and manage email outbound campaigns
- Target specific lead tiers and score ranges
- Set daily sending limits
- Custom email templates with variable substitution
- Real-time campaign statistics (sent, opens, clicks, replies)
- Activate, pause, and test campaigns
- Track campaign performance

## Quick Start

### 1. Install Dependencies

```bash
# Make sure you're in the lead-enrichment directory
cd /Users/frisokolkman/Desktop/friso_ai/lead-enrichment

# Run the startup script
./start.sh
```

### 2. Configure Environment

Edit your `.env` file with the necessary configuration:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/lead_enrichment

# API Keys (for enrichment providers)
APOLLO_API_KEY=your_key_here
CLEARBIT_API_KEY=your_key_here
HUNTER_API_KEY=your_key_here
# ... other providers

# Budget
MONTHLY_BUDGET=1000

# CRM Integration
ATTIO_API_KEY=your_key_here

# Email
RESEND_API_KEY=your_key_here
```

### 3. Initialize Database

```bash
# Run database migrations
alembic upgrade head
```

### 4. Start the Application

```bash
# Using the startup script (recommended)
./start.sh

# Or manually with uvicorn
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### 5. Access the CRM

Open your browser and navigate to:
```
http://localhost:8000
```

## Navigation

- **Dashboard** - Overview of your pipeline with key metrics
- **Leads** - Browse, filter, and manage all leads
- **Campaigns** - Create and manage outbound email campaigns
- **Analytics** - View detailed analytics (coming soon)

## Using the CRM

### Viewing Leads

1. Click "Leads" in the navigation
2. Use the filter button to refine by:
   - Status (NEW, ENRICHING, ENRICHED, SCORED, etc.)
   - Tier (High Touch, Standard, Nurture)
   - Minimum score threshold
3. Click on any lead to view detailed information

### Creating a Campaign

1. Navigate to the "Campaigns" page
2. Click "Create Campaign"
3. Fill in the campaign details:
   - Name and description
   - Target tier (optional)
   - Score range (optional)
   - Daily sending limit
   - Email templates with variables
4. Click "Create Campaign"
5. Activate the campaign when ready

### Running the Pipeline

From the Dashboard, use the Quick Actions:
- **Run Enrichment (1-5)** - Enrich company and contact data
- **Run AI Research (6-7)** - Generate research summaries and messaging
- **Sync to CRM (8)** - Sync enriched leads to your CRM

Or use the API endpoint:
```bash
POST /api/pipeline/run?start_stage=1&end_stage=9&limit=100
```

## API Endpoints

The CRM provides a full REST API:

### Leads
- `GET /api/leads` - List leads with filters
- `GET /api/leads/{id}` - Get lead details
- `GET /api/status` - Get pipeline status
- `GET /api/budget` - Get budget status

### Campaigns
- `GET /api/campaigns` - List all campaigns
- `POST /api/campaigns` - Create new campaign
- `POST /api/campaigns/{id}/activate` - Activate campaign
- `POST /api/campaigns/{id}/pause` - Pause campaign
- `POST /api/campaigns/{id}/launch` - Launch campaign

### Pipeline
- `POST /api/pipeline/run` - Run pipeline stages

### Statistics
- `GET /api/stats/scoring` - Scoring statistics
- `GET /api/stats/verification` - Email verification statistics
- `GET /api/stats/messaging` - Messaging generation statistics
- `GET /api/stats/sync` - CRM sync statistics

## Technology Stack

- **Backend**: FastAPI (Python)
- **Frontend**: HTML, Tailwind CSS, Alpine.js, HTMX
- **Database**: PostgreSQL with SQLAlchemy ORM
- **API Integrations**: Apollo, Clearbit, Hunter, Prospeo, DropContact, ZeroBounce
- **CRM**: Attio
- **Email**: Resend

## Directory Structure

```
lead-enrichment/
├── app.py                 # FastAPI application
├── main.py               # CLI entry point
├── config.py             # Configuration
├── requirements.txt      # Python dependencies
├── start.sh             # Startup script
├── templates/           # HTML templates
│   ├── dashboard.html
│   ├── leads.html
│   ├── lead_detail.html
│   └── campaigns.html
├── core/                # Core models and database
│   ├── models.py
│   ├── database.py
│   └── schemas.py
├── enrichment/          # Enrichment providers
├── pipeline/            # Pipeline stages
├── integrations/        # CRM and email integrations
├── ai/                  # AI research and generation
└── utils/              # Utilities (rate limiting, cost tracking)
```

## Troubleshooting

### Database Connection Issues
Make sure PostgreSQL is running and the DATABASE_URL in `.env` is correct:
```bash
# Test database connection
psql $DATABASE_URL
```

### Port Already in Use
If port 8000 is already in use, specify a different port:
```bash
uvicorn app:app --host 0.0.0.0 --port 8001 --reload
```

### Missing Dependencies
Reinstall all dependencies:
```bash
pip install -r requirements.txt --upgrade
```

## Support

For issues or questions, refer to the main project documentation or create an issue in the project repository.

## License

See main project LICENSE file.
