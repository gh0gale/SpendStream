import pdfplumber

def read_pdf(file_path):
    """
    Extracts raw text from all pages of a PDF
    """
    text = ""
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text
