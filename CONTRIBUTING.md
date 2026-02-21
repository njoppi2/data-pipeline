# Contributing

## Setup

1. Copy env templates:

```bash
cp .env.example .env
cp dags/.env.example dags/.env
```

2. Start local stack:

```bash
make start
```

## Before Opening a PR

Run:

```bash
python -m compileall dags utils
docker compose -f docker-compose.yml config > /dev/null
python utils/validate_data_contracts.py \
  --contract contracts/order_details.contract.json \
  --csv data/order_details.csv
```

## Pull Request Guidelines

- Keep PR scope focused (single feature/fix).
- Update `README.md` when behavior or commands change.
- Include validation evidence (command output or CI link).
- Never commit real `.env` files or credentials.
