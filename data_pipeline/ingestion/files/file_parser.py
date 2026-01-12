from data_pipeline.ingestion.common.schema import raw_transaction

def parse_csv_row(row, filename):
    """
    Converts a CSV row into raw transaction format
    """
    return raw_transaction(
        source="file_csv",
        sender=filename,
        timestamp=row.get("date", ""),
        raw_text=str(row),
        metadata={
            "file_type": "csv"
        }
    )

def parse_pdf_text(text, filename):
    """
    Converts PDF text into raw transaction format
    """
    return raw_transaction(
        source="file_pdf",
        sender=filename,
        timestamp="",
        raw_text=text,
        metadata={
            "file_type": "pdf"
        }
    )
