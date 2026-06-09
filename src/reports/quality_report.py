"""
Quality report - Readable format from the result of our quality checks.
"""
from datetime import datetime
from pathlib import Path
import json


def format_number(n: int) -> str:
    """1234567 -> '1,234,567'"""
    return f"{n:,}"


def format_percent(value: float, total: int) -> str:
    """Calculates a percent %"""
    if total == 0:
        return "0.0%"
    return f"{(value / total * 100):.2f}%"


def print_section_header(title: str) -> None:
    """Colorful header for section"""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def print_quality_report(results: dict) -> None:
    """
    Prints formated quality report in the consol

    Args:
        results: dict от run_all_checks()
    """
    total = results["summary"]["total_rows"]

    print(f"\n{'#'*70}")
    print(f"#  NYC YELLOW TAXI - DATA QUALITY REPORT")
    print(f"#  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'#'*70}")

    # ==================
    # SUMMARY
    # ==================
    print_section_header("SUMMARY")
    print(f"  Total rows:    {format_number(total)}")
    print(f"  Total columns: {results['summary']['total_columns']}")
    print(f"  Duplicates:    {format_number(results['duplicates']['full_duplicates'])}")

    # ==================
    # NULLS
    # ==================
    print_section_header("NULL VALUES (only columns with nulls)")
    nulls = results["nulls"]["counts"]
    percentages = results["nulls"]["percentages"]

    columns_with_nulls = {c: n for c, n in nulls.items() if n > 0}

    if not columns_with_nulls:
        print("  ✓ No nulls found")
    else:
        print(f"  {'Column':<35} {'Count':>15} {'Percent':>10}")
        print(f"  {'-'*35} {'-'*15} {'-'*10}")
        for col_name, null_count in sorted(
            columns_with_nulls.items(),
            key=lambda x: -x[1]
        ):
            pct = percentages[col_name]
            print(f"  {col_name:<35} {format_number(null_count):>15} {pct:>9.2f}%")

    # ==================
    # PROBLEMS BY CATEGORY
    # ==================
    categories = [
        ("NEGATIVE VALUES", results["negative_values"]),
        ("ZERO VALUES", results["zero_values"]),
        ("OUTLIERS", results["outliers"]),
        ("DATE ISSUES", results["dates"]),
        ("DURATION ISSUES", results["duration"]),
        ("CATEGORICAL ISSUES", results["categories"]),
    ]

    for category_name, category_data in categories:
        print_section_header(category_name)

        problems = {k: v for k, v in category_data.items() if v > 0}

        if not problems:
            print("  ✓ No issues found")
            continue

        print(f"  {'Check':<35} {'Count':>15} {'Percent':>10}")
        print(f"  {'-'*35} {'-'*15} {'-'*10}")
        for check_name, count_val in sorted(
            problems.items(),
            key=lambda x: -x[1]
        ):
            pct = format_percent(count_val, total)
            print(f"  {check_name:<35} {format_number(count_val):>15} {pct:>10}")

    # ==================
    # OVERALL VERDICT
    # ==================
    print_section_header("OVERALL VERDICT")

    total_problems = sum([
        sum(results["negative_values"].values()),
        sum(results["zero_values"].values()),
        sum(results["outliers"].values()),
        sum(results["dates"].values()),
        sum(results["duration"].values()),
        sum(results["categories"].values()),
    ])

    problem_pct = (total_problems / total * 100) if total > 0 else 0

    print(f"  Total problem records: {format_number(total_problems)}")
    print(f"  Problem percentage:    {problem_pct:.2f}%")

    if problem_pct < 1:
        verdict = "✓ EXCELLENT - data is very clean"
    elif problem_pct < 5:
        verdict = "○ GOOD - typical for real-world data"
    elif problem_pct < 15:
        verdict = "△ FAIR - significant cleaning needed"
    else:
        verdict = "✗ POOR - investigate data source"

    print(f"  Verdict: {verdict}")
    print(f"\n{'#'*70}\n")


def save_quality_report_json(results: dict, output_dir: str = "data/reports") -> Path:
    """
    Saves the report in a json file, for future use and comparison

    Returns:
        Path to the saved file
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_path / f"quality_report_{timestamp}.json"

    # Adds timestamp to the results
    results_with_meta = {
        "timestamp": datetime.now().isoformat(),
        **results
    }

    with open(filename, "w") as f:
        json.dump(results_with_meta, f, indent=2)

    return filename