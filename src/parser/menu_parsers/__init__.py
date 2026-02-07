"""
Menu parsers for different formats and platforms.
"""
from .extruct_parser import ExtructParser
from .html_llm_parser import HtmlLlmParser
from .pdf_parser import PdfParser
from .image_parser import ImageParser

__all__ = [
    'ExtructParser',
    'HtmlLlmParser',
    'PdfParser',
    'ImageParser',
]
