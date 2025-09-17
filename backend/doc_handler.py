import PyPDF2
import docx

def extract_pdf_text(filepath):
    from PyPDF2 import PdfReader
    with open(filepath, "rb") as f:
        reader = PdfReader(f)
        return " ".join([page.extract_text() or "" for page in reader.pages])

def extract_docx_text(filepath):
    doc = docx.Document(filepath)
    return " ".join(para.text for para in doc.paragraphs)
