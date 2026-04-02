"""Utility functions for the table flow pipeline."""

import re


def convert_number(value: str) -> str | float | None:
    """Convert a numeric string with unit suffixes into a plain number.

    Supported Chinese units: 千, 万, 十万, 百万, 千万, 亿, 十亿, 百亿, 千亿, 万亿
    Supported English units: K, Thousand, Million, M, Billion, B, Trillion, T

    Only converts when the input matches the "number + unit" pattern;
    otherwise returns the original input unchanged.

    Args:
        value: A numeric string with optional unit, e.g. "1百万", "1.5亿", "2.3 million", "1.2B".

    Returns:
        The converted number (float), or the original string if the format is unrecognized.

    Examples:
        >>> convert_number("1百万")
        1000000.0
        >>> convert_number("1.5亿")
        150000000.0
        >>> convert_number("2.3 million")
        2300000.0
        >>> convert_number("苹果公司")
        '苹果公司'
    """
    # Chinese numeric unit mapping
    cn_unit_map = {
        "千": 1_000,
        "万": 10_000,
        "十万": 100_000,
        "百万": 1_000_000,
        "千万": 10_000_000,
        "亿": 100_000_000,
        "十亿": 1_000_000_000,
        "百亿": 10_000_000_000,
        "千亿": 100_000_000_000,
        "万亿": 1_000_000_000_000,
    }

    # English numeric unit mapping (lowercase)
    en_unit_map = {
        "k": 1_000,
        "thousand": 1_000,
        "million": 1_000_000,
        "m": 1_000_000,
        "billion": 1_000_000_000,
        "b": 1_000_000_000,
        "trillion": 1_000_000_000_000,
        "t": 1_000_000_000_000,
    }

    if value is None:
        return None

    original_value = str(value).strip()
    if not original_value:
        return original_value

    # Remove thousands-separator commas for numeric parsing
    cleaned = original_value.replace(",", "")

    # Try direct numeric conversion (plain number string)
    try:
        return float(cleaned)
    except ValueError:
        pass

    # Build regex for Chinese units: number + unit at end
    cn_units_pattern = "|".join(sorted(cn_unit_map.keys(), key=len, reverse=True))
    cn_pattern = rf"^([+-]?\d+(?:\.\d+)?)\s*({cn_units_pattern})$"
    cn_match = re.match(cn_pattern, cleaned)
    if cn_match:
        num_str, unit = cn_match.groups()
        try:
            return float(num_str) * cn_unit_map[unit]
        except ValueError:
            pass

    # Build regex for English units: number + unit (case-insensitive)
    en_units_pattern = "|".join(sorted(en_unit_map.keys(), key=len, reverse=True))
    en_pattern = rf"^([+-]?\d+(?:\.\d+)?)\s*({en_units_pattern})$"
    en_match = re.match(en_pattern, cleaned, re.IGNORECASE)
    if en_match:
        num_str, unit = en_match.groups()
        try:
            return float(num_str) * en_unit_map[unit.lower()]
        except ValueError:
            pass

    # Not a "number + unit" format; return original input
    return original_value


def generate_time_values(time_dimension: dict) -> list[str]:
    """Generate a list of time-period strings from a time dimension specification.

    Args:
        time_dimension: A dict with keys like type, format, start, end, values.

    Returns:
        A list of time-period strings.
    """
    td_type = time_dimension.get("type")

    if td_type == "explicit":
        # Use the values directly
        return time_dimension.get("values", [])

    elif td_type == "year":
        # Increment by year
        start_year = int(time_dimension.get("start"))
        end_year = int(time_dimension.get("end"))
        return [str(year) for year in range(start_year, end_year + 1)]

    elif td_type == "quarter":
        # Increment by quarter
        values = []
        start = time_dimension.get("start", "")
        end = time_dimension.get("end", "")

        # Parse start: "YYYY Qn" or "YYYY"
        start_parts = start.split()
        if len(start_parts) == 2:
            start_year = int(start_parts[0])
            start_quarter = int(start_parts[1][1])  # "Q1" -> 1
        else:
            start_year = int(start_parts[0])
            start_quarter = 1

        # Parse end: "YYYY Qn" or "YYYY"
        end_parts = end.split()
        if len(end_parts) == 2:
            end_year = int(end_parts[0])
            end_quarter = int(end_parts[1][1])  # "Q4" -> 4
        else:
            end_year = int(end_parts[0])
            end_quarter = 4

        current_year = start_year
        current_quarter = start_quarter

        while (current_year < end_year) or (current_year == end_year and current_quarter <= end_quarter):
            values.append(f"{current_year} Q{current_quarter}")
            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1

        return values

    return []
