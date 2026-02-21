# Data Contracts

This repository now includes explicit data contracts for extracted CSV artifacts.

## Contract Files

- `contracts/order_details.contract.json`

Each contract can define:

- `required_columns`: columns that must exist
- `non_nullable_columns`: columns that cannot be empty
- `numeric_columns`: numeric type and range constraints
- `min_rows`: minimum number of data rows

## Validation Command

Run contract checks locally:

```bash
python utils/validate_data_contracts.py \
  --contract contracts/order_details.contract.json \
  --csv data/order_details.csv
```

## CI Integration

`ci.yml` executes the same validation command to ensure fixture data still satisfies the contract.
