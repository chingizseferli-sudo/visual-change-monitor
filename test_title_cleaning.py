import sys
import types

sys.modules.setdefault("dotenv", types.SimpleNamespace(load_dotenv=lambda *args, **kwargs: None))
from change_monitor import clean_item_title


def check(raw, expected, published=""):
    actual = clean_item_title(raw, published)
    assert actual == expected, f"{raw!r} -> {actual!r}, expected {expected!r}"


def main():
    check(
        "ATU-da təhsil alan əcnəbi tələbələrdən maraqlı təşəbbüs – Məzun günü",
        "ATU-da təhsil alan əcnəbi tələbələrdən maraqlı təşəbbüs Məzun günü",
    )
    check(
        "DİM-də vacib xəbər",
        "DİM-də vacib xəbər",
    )
    check(
        "17:47 DİM-də vacib xəbər",
        "DİM-də vacib xəbər",
    )
    check(
        "29.06.2026 17:47 ATU-da təhsil alan əcnəbi tələbələrdən maraqlı təşəbbüs",
        "ATU-da təhsil alan əcnəbi tələbələrdən maraqlı təşəbbüs",
    )
    check(
        "Xəbərlər: DİM-də vacib xəbər",
        "DİM-də vacib xəbər",
    )
    check(
        "Təhsil - DİM-də vacib xəbər",
        "DİM-də vacib xəbər",
    )


if __name__ == "__main__":
    main()