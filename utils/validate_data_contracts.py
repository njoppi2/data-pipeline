#!/usr/bin/env python3
import argparse
import csv
import json
import sys
from pathlib import Path


class ContractValidationError(Exception):
    pass


def load_contract(contract_path: Path) -> dict:
    with contract_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _is_empty(value: str | None) -> bool:
    return value is None or value.strip() == ""


def validate_csv_against_contract(contract: dict, csv_path: Path) -> None:
    required_columns = contract.get("required_columns", [])
    non_nullable_columns = set(contract.get("non_nullable_columns", []))
    numeric_columns = contract.get("numeric_columns", {})
    min_rows = int(contract.get("min_rows", 1))

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)

        if not reader.fieldnames:
            raise ContractValidationError("CSV header is missing.")

        duplicate_columns = [
            column
            for index, column in enumerate(reader.fieldnames)
            if column in reader.fieldnames[:index]
        ]
        if duplicate_columns:
            duplicated = ", ".join(sorted(set(duplicate_columns)))
            raise ContractValidationError(f"Duplicate column names detected: {duplicated}")

        missing_columns = [column for column in required_columns if column not in reader.fieldnames]
        if missing_columns:
            missing = ", ".join(missing_columns)
            raise ContractValidationError(f"Missing required column(s): {missing}")

        row_count = 0
        for line_number, row in enumerate(reader, start=2):
            row_count += 1

            for column in non_nullable_columns:
                if _is_empty(row.get(column)):
                    raise ContractValidationError(
                        f"Row {line_number}: non-nullable column '{column}' is empty."
                    )

            for column, rule in numeric_columns.items():
                raw_value = row.get(column)
                if _is_empty(raw_value):
                    raise ContractValidationError(
                        f"Row {line_number}: numeric column '{column}' is empty."
                    )

                try:
                    numeric_value = float(raw_value)
                except ValueError as exc:
                    raise ContractValidationError(
                        f"Row {line_number}: '{column}' value '{raw_value}' is not numeric."
                    ) from exc

                if rule.get("type") == "int" and not numeric_value.is_integer():
                    raise ContractValidationError(
                        f"Row {line_number}: '{column}' value '{raw_value}' is not an integer."
                    )

                minimum = rule.get("min")
                if minimum is not None and numeric_value < float(minimum):
                    raise ContractValidationError(
                        f"Row {line_number}: '{column}' value {numeric_value} < min {minimum}."
                    )

                maximum = rule.get("max")
                if maximum is not None and numeric_value > float(maximum):
                    raise ContractValidationError(
                        f"Row {line_number}: '{column}' value {numeric_value} > max {maximum}."
                    )

        if row_count < min_rows:
            raise ContractValidationError(
                f"CSV has {row_count} row(s), below minimum required {min_rows}."
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate CSV data against a JSON data contract.")
    parser.add_argument("--contract", required=True, help="Path to contract JSON file")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    args = parser.parse_args()

    contract_path = Path(args.contract)
    csv_path = Path(args.csv)

    if not contract_path.is_file():
        print(f"Contract file not found: {contract_path}", file=sys.stderr)
        return 1

    if not csv_path.is_file():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 1

    try:
        contract = load_contract(contract_path)
        validate_csv_against_contract(contract, csv_path)
    except (json.JSONDecodeError, ContractValidationError) as exc:
        print(f"Contract validation failed: {exc}", file=sys.stderr)
        return 1

    print(f"Contract validation passed for {csv_path} using {contract_path}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
