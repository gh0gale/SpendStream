# Real bank alert fixtures

`backend/test_gmail_parser.py` runs every `*.json` file in this folder through `gmail_parser.parse_bank_alert` and compares the result with `expected`. The synthetic cases in the test only pin down what the regexes do. These files are the only evidence that the parser reads real bank emails correctly.

Add at least one file for each bank the Gmail search covers: HDFC, ICICI, SBI, Axis, Kotak, Yes Bank. Also add non-debit emails from the same senders (OTPs, credits, statements), with `"expected": null`.

## Format

One email per file, for example `hdfc_upi_debit_01.json`:

```json
{
  "bank": "HDFC",
  "note": "UPI debit alert, received September 2026",
  "body": "<the plain-text body of the email, anonymised>",
  "expected": { "amount": 250.0, "receiver": "VPA somebody@okaxis" }
}
```

- `bank` must be spelled as in the list above, so the test can report which banks are covered.
- `body` is the email text as the parser sees it: the `text/plain` part if the email has one, otherwise the `text/html` part.
- `expected` is what a correct parser should return. Use `null` for emails that are not debit alerts.

## Anonymise before adding

Replace your name, account and card numbers, UPI ids, phone numbers and reference numbers with made-up values of the same shape. For example, keep `XX1234` looking like `XX` plus four digits, and keep a UPI id looking like `name@bank`. Leave the wording, spacing and line breaks exactly as the bank sent them, because that is what the parser depends on.
