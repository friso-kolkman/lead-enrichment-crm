# Lead Enrichment CRM

A comprehensive lead enrichment pipeline with a modern web-based CRM interface for managing B2B sales leads.

![CRM Dashboard](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)

## Overview

This system provides a complete 9-stage waterfall pipeline for lead enrichment, scoring, and outbound campaigns with a beautiful web interface for managing your sales pipeline.

### Key Features

- **🔄 Automated Lead Enrichment** - Multi-provider cascade for company and contact data
- **🎯 AI-Powered Scoring** - Intelligent lead scoring and tier assignment
- **🧠 AI Research** - Automated research summaries and personalized messaging
- **📧 Campaign Management** - Create and manage email outbound campaigns
- **💰 Budget Tracking** - Real-time cost monitoring across providers
- **🔗 CRM Integration** - Sync enriched leads to Attio CRM
- **📊 Web Dashboard** - Modern CRM interface for lead management

## Screenshots

### Dashboard
Overview of your pipeline with real-time statistics and quick actions.

### Leads Management
Browse, filter, and manage leads with detailed company and contact information.

### Lead Details
Comprehensive lead profiles with enrichment data, AI research, and personalized messaging.

### Campaign Management
Create and monitor email campaigns with targeting and performance tracking.

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL database
- API keys for enrichment providers (Apollo, Clearbit, Hunter, etc.)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/lead-enrichment.git
cd lead-enrichment
```

2. Set up environment:
```bash
cp .env.example .env
# Edit .env with your API keys and configuration
```

3. Run the startup script:
```bash
./start.sh
```

4. Access the CRM at `http://localhost:8000`

## Pipeline Stages

1. **Ingestion** - Import leads from CSV or API
2. **Company Enrichment** - Enrich with firmographics and technographics
3. **Contact Enrichment** - Find decision-makers and contact info
4. **Email Verification** - Verify email deliverability
5. **Scoring** - Calculate lead scores and assign tiers
6. **AI Research** - Generate research summaries and insights
7. **Messaging** - Create personalized email templates
8. **CRM Sync** - Sync to Attio CRM
9. **Campaign** - Launch outbound email campaigns

## Technology Stack

- **Backend**: FastAPI, SQLAlchemy, PostgreSQL
- **Frontend**: HTML, Tailwind CSS, Alpine.js, HTMX
- **Enrichment**: Apollo, Clearbit, Hunter, Prospeo, DropContact, ZeroBounce
- **AI**: Claude (Anthropic API) for research and messaging
- **CRM**: Attio integration
- **Email**: Resend for sending campaigns

## Configuration

Key environment variables in `.env`:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/lead_enrichment

# Enrichment Providers
APOLLO_API_KEY=your_key
CLEARBIT_API_KEY=your_key
HUNTER_API_KEY=your_key
PROSPEO_API_KEY=your_key
DROPCONTACT_API_KEY=your_key
ZEROBOUNCE_API_KEY=your_key

# AI
ANTHROPIC_API_KEY=your_key

# CRM
ATTIO_API_KEY=your_key

# Email
RESEND_API_KEY=your_key

# Budget
MONTHLY_BUDGET=1000
```

## API Documentation

Once running, access the interactive API docs at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Key Endpoints

- `GET /` - Dashboard
- `GET /leads` - Leads list with filtering
- `GET /leads/{id}` - Lead detail page
- `GET /campaigns` - Campaign management
- `POST /api/pipeline/run` - Run pipeline stages
- `POST /api/campaigns` - Create campaign
- `GET /api/leads` - List leads (JSON API)

## Usage

### Import Leads

```bash
python main.py ingest leads.csv
```

### Run Pipeline

```bash
# Full pipeline
python main.py run --start 1 --end 9

# Or use the web dashboard
# Click "Run Enrichment" or "Run AI Research" buttons
```

### Create Campaign

1. Navigate to `/campaigns`
2. Click "Create Campaign"
3. Set targeting criteria (tier, score range)
4. Add email templates
5. Activate campaign

## Development

### Project Structure

```
lead-enrichment/
├── app.py                 # FastAPI web application
├── main.py               # CLI entry point
├── config.py             # Configuration management
├── templates/            # HTML templates
│   ├── dashboard.html
│   ├── leads.html
│   ├── lead_detail.html
│   └── campaigns.html
├── core/                 # Core models and database
├── enrichment/           # Enrichment provider integrations
├── pipeline/             # Pipeline stage implementations
├── integrations/         # CRM and email integrations
├── ai/                   # AI research and generation
└── utils/               # Utilities and helpers
```

### Running Tests

```bash
pytest
```

### Database Migrations

```bash
# Create migration
alembic revision --autogenerate -m "description"

# Apply migrations
alembic upgrade head
```

## Cost Management

The system tracks costs across all providers and enforces monthly budgets:

- Real-time cost tracking per provider
- Monthly budget limits
- Cost breakdown in dashboard
- Automatic rate limiting

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Support

For questions or issues, please open an issue on GitHub.

## Roadmap

- [ ] Additional enrichment providers
- [ ] LinkedIn automation
- [ ] A/B testing for email campaigns
- [ ] Advanced analytics dashboard
- [ ] Webhook integrations
- [ ] Multi-user support with authentication
- [ ] Custom scoring models

---

Built with ❤️ using FastAPI and modern web technologies
