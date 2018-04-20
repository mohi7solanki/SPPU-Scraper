import datetime

import tablib


def save_result(fetched_results, output='xlsx'):
    subjects = [''.join(subject.keys()) for subject in fetched_results[0].grades]
    data = tablib.Dataset(
        headers=("No", "Seat No", "Name", "SGPA", "Backlogs", *subjects)
    )
    for i, student in enumerate(fetched_results, 1):
        subject_grades = (
            subject["grade"] for grade in student.grades for subject in grade.values()
        )
        sgpa = '-' if not student.sgpa else student.sgpa
        backlogs = '-' if not student.backlogs else '\n'.join(student.backlogs)
        row = (i, student.seat_no, student.name, sgpa, backlogs, *subject_grades)
        data.append(row)
    datetime_append = datetime.datetime.now().strftime('%d-%m-%y-%H-%M-%S')
    file_name = 'results-{}.{}'.format(datetime_append, output)
    with open(file_name, 'wb') as file:
        file.write(data.export(output))
    return file_name


def sort_students(fetched_results, by="seat_no", reverse=False):

    def sort_key(student):
        if by == "seat_no":
            return student.seat_no

        if by == "sgpa":
            credits = sum(
                int(subject["credits"])
                for grade in student.grades
                for subject in grade.values()
            )
            return student.sgpa, credits, -int(student.seat_no[1:])

    return sorted(fetched_results, key=sort_key, reverse=reverse)


def generate_form_data(soup, seat_no, mother_name):
    __EVENTVALIDATION = soup.find(id="__EVENTVALIDATION").get("value")
    __VIEWSTATE = soup.find(id="__VIEWSTATE").get("value")
    __PREVIOUSPAGE = soup.find(id="__PREVIOUSPAGE").get("value")
    __VIEWSTATEGENERATOR = soup.find(id="__VIEWSTATEGENERATOR").get("value")
    form_data = {
        "ctl00$ContentPlaceHolder1$btnSubmit": "Show Result",
        "ctl00$ContentPlaceHolder1$txtMother": mother_name,
        "ctl00$ContentPlaceHolder1$txtSeatno": seat_no,
        "__EVENTARGUMENT": None,
        "__EVENTTARGET": None,
        "__EVENTVALIDATION": __EVENTVALIDATION,
        "__VIEWSTATE": __VIEWSTATE,
        "__PREVIOUSPAGE": __PREVIOUSPAGE,
        "__VIEWSTATEGENERATOR": __VIEWSTATEGENERATOR,
    }
    return form_data
