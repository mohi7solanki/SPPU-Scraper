import re
from collections import namedtuple

import PyPDF2


regex = re.compile(r'([FSTB]\d{9})\s+((?:\w+\s\.?)+)\s{2,}((?:\w+\s)+)')

Student = namedtuple('student', 'seat_no name mother_name')

all_students = []


class NoStudentsFoundError(Exception):
    """No students are found"""
    pass


def save_match(all_matches):
    for match in all_matches:
        seat_no, name, mother_name = map(getattr(str, 'strip'), (*match,))
        student = Student(seat_no, name, mother_name)
        all_students.append(student)


def get_student_data(filepath):
    with open(filepath, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        for page_no in range(pdf_reader.getNumPages()):
            page_obj = pdf_reader.getPage(page_no)
            data = page_obj.extractText()
            save_match(regex.findall(data))
    if not all_students:
        raise NoStudentsFoundError('No student details found in the given file.')
    return all_students
