# Industrial Engineering Agent

A maintenance analytics system that analyzes mechanic repair data and provides insights into performance metrics.

## Features

- Analyzes mechanic repair times and response times
- Identifies performance gaps between mechanics
- Generates detailed reports by machine type and failure reason
- Stores findings in a Supabase database

## Project Structure

```
src/
├── agents/
│   └── maintenance/
│       └── analytics/
│           └── Mechanic_ave/
│               ├── mechanic_repair_analyzer.py
│               └── mechanic_repair_interpreter.py
└── shared_services/
    └── db_client.py
```

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Create a `.env.local` file in the `src` directory with your Supabase credentials:
   ```
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   ```

## Usage

1. Run the analyzer:
   ```bash
   python src/agents/maintenance/analytics/Mechanic_ave/mechanic_repair_analyzer.py
   ```

2. Run the interpreter:
   ```bash
   python src/agents/maintenance/analytics/Mechanic_ave/mechanic_repair_interpreter.py
   ```

## Requirements

- Python 3.8+
- pandas
- numpy
- scikit-learn
- firebase-admin
- python-dotenv
- psycopg2-binary

## License

MIT 