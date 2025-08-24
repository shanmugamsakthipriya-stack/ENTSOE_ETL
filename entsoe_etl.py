name: ENTSOE ETL

on:
  # Manual trigger
  workflow_dispatch:
    inputs:
      run_type:
        description: 'Choose run type: historical or daily'
        required: true
        default: 'daily'
        type: choice
        options:
          - daily
          - historical

  # Scheduled daily at 02:00 UTC
  schedule:
    - cron: '0 2 * * *'  # daily 02:00 UTC

jobs:
  run-etl:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests pandas psycopg2-binary pytz

      - name: Run ETL
        env:
          ENTSOE_TOKEN: ${{ secrets.ENTSOE_TOKEN }}
          AZURE_PG_HOST: ${{ secrets.AZURE_PG_HOST }}
          AZURE_PG_DB: ${{ secrets.AZURE_PG_DB }}
          AZURE_PG_USER: ${{ secrets.AZURE_PG_USER }}
          AZURE_PG_PASSWORD: ${{ secrets.AZURE_PG_PASSWORD }}
        run: |
          if [ "${{ github.event.inputs.run_type }}" = "historical" ]; then
            echo "Running historical load..."
            python entsoe_etl.py --historical
          else
            echo "Running daily load..."
            python entsoe_etl.py
          fi
