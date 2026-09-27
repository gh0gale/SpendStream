"""
Offline checks for export_demo.py: what reaches the public site is anonymised.
No database, network or .env. Run from backend/: python test_export_demo.py
"""

import sys

from export_demo import breakdown, display_name, person_names, pick_month, scrub_alert


def row(**kw):
    base = {"id": "x", "merchant": "Swiggy", "merchant_key": "swiggy@axisbank",
            "merchant_is_person": False, "transaction_date": "2026-08-03",
            "category": "Food", "is_categorised": True, "user_corrected": False,
            "amount": 100}
    return {**base, **kw}


def test_scrub_alert_removes_account_and_reference_digits():
    text = ("Dear Customer,\n\nRs.450.00 has been debited from account **4821 to VPA "
            "swiggy@axisbank SWIGGY on 03-08-26. Your UPI transaction reference "
            "number is 523412345678.")
    out = scrub_alert(text)
    assert "4821" not in out and "523412345678" not in out, out
    assert "**0000" in out and "Rs.450.00" in out and "swiggy@axisbank" in out, out


def test_scrub_alert_removes_account_ending_digits():
    text = ("Dear Customer, Greetings from HDFC Bank! Rs.1994.50 is debited from your account "
            "ending 9608 towards VPA irctcrailweb@axl (IRCTC Rail Web) on 13-07-26. UPI "
            "transaction reference no.: 523412345678. If you did not authorize this")
    out = scrub_alert(text)
    assert "9608" not in out and "523412345678" not in out, out
    assert "Rs.1994.50" in out and "13-07-26" in out, out
    assert out.endswith("."), out


def test_people_are_renamed_in_order_of_first_payment():
    rows = [row(merchant="Ravi K", merchant_key="ravi@okicici", merchant_is_person=True,
                transaction_date="2026-08-09"),
            row(merchant="Anu S", merchant_key="anu@ybl", merchant_is_person=True,
                transaction_date="2026-08-01"),
            row()]
    names = person_names(rows)
    assert names == {"anu@ybl": "Person A", "ravi@okicici": "Person B"}, names
    assert display_name(rows[0], names) == "Person B"
    assert display_name(rows[2], names) == "Swiggy"


def test_breakdown_keeps_unsure_apart_and_sorts_by_total():
    cats, unsure = breakdown([row(amount=100), row(category="Transport", amount=300),
                              row(category=None, is_categorised=False, amount=50)])
    assert [c["category"] for c in cats] == ["Transport", "Food"], cats
    assert unsure == {"total": 50.0, "count": 1}, unsure


def test_pick_month_prefers_the_fullest_past_month():
    rows = [row(transaction_date="2020-01-05")] * 2 + [row(transaction_date="2020-02-05")]
    assert pick_month(rows, None) == "2020-01"
    assert pick_month(rows, "2020-02") == "2020-02"


if __name__ == "__main__":
    tests = [v for k, v in dict(globals()).items() if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
    print(f"{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
