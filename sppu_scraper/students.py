import re
from collections import namedtuple

import PyPDF2


regex = re.compile(r'([FSTB]\d{9})\s+((?:\w+\s\.?)+)\s{2,}((?:\w+\s)+)')

Student = namedtuple('student', 'seat_no name mother_name')

all_students = []


def save_match(all_matches):
    for match in all_matches:
        seat_no, name, mother_name = map(
            lambda x: x.strip(), (match[0], match[1], match[2])
        )
        student = Student(seat_no, name, mother_name)
        all_students.append(student)


def get_student_data(filepath):
    with open(filepath, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfFileReader(pdf_file)
        for page_no in range(pdf_reader.getNumPages()):
            page_obj = pdf_reader.getPage(page_no)
            data = page_obj.extractText()
            all_matches = regex.findall(data)
            save_match(all_matches)
    return all_students
