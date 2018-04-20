import sys
import threading
import warnings
from collections import namedtuple, Counter
from concurrent.futures import as_completed, ThreadPoolExecutor
from itertools import chain

import requests
from bs4 import BeautifulSoup
from requests.exceptions import ConnectionError, ConnectTimeout
from PyPDF2.utils import PdfReadError

from .students import get_student_data
from .utils import generate_form_data, save_result, sort_students


# Supress openpyxl warnings.
warnings.simplefilter("ignore", category=UserWarning)

Result = namedtuple('result', 'seat_no name grades sgpa backlogs')

result_fetched = set()

next_year = {'F': 'S', 'S': 'T', 'T': 'B'}

lock = threading.RLock()


def get_student_report_card(soup):
    result_set, backlogs = [], []
    table = soup.find_all('table')[1]
    data = table.find_all('tr', {'align': 'LEFT'})
    sgpa = table.find_all('tr')[-3].text.strip().split(':-')[1].strip()
    name = soup.table.findAll('td')[2].text.split(':')[1].strip()
    for row in data:
        all_td = row.find_all('td')
        grade = all_td[-2].text.strip()
        credit_points = all_td[-1].text.strip()
        _type = all_td[3].text.strip()
        subject = all_td[2].text.strip()
        sub_dict = {}
        sub_dict[subject] = {'type': _type, 'grade': grade, 'credits': credit_points}
        if grade == 'F':
            backlogs.append(subject)
        result_set.append(sub_dict)
    try:
        sgpa = float(sgpa)
    except ValueError:
        sgpa = 0
    return name, result_set, sgpa, backlogs


def get_search_space(seat_number):
    OFFSET = 18
    roll_no = int(seat_number[-2:])
    min_num = roll_no - OFFSET
    min_num = min_num if min_num > 0 else 0
    max_num = roll_no + 10
    prepend_number = next_year[seat_number[0]] + seat_number[1:8]
    search_space = {
        prepend_number + str(number).zfill(2) for number in range(min_num, max_num + 1)
    }
    return search_space - result_fetched


def fetch_details(pg_source, seat_number):
    soup = BeautifulSoup(pg_source, 'lxml')
    name, grades, sgpa, backlogs = get_student_report_card(soup)
    return Result(seat_number, name, grades, sgpa, backlogs)


def get_result_current(student):
    form_data = generate_form_data(soup, student.seat_no, student.mother_name)
    try:
        res = requests.post(result_home_page, data=form_data, timeout=7)
    except Exception:
        return None, student
    return True, fetch_details(res.text, student.seat_no)


def get_result_previous(student, search_space=None):
    if search_space is None:
        search_space = get_search_space(student.seat_no)
    if search_space:
        seat_number = search_space.pop()
        mother_name = student.mother_name
        form_data = generate_form_data(soup, seat_number, mother_name)
        try:
            res = requests.post(result_home_page, data=form_data, timeout=7)
        except Exception:
            return None, student
        else:
            if 'SUBJECT NAME' in res.text:
                lock.acquire()
                if seat_number in result_fetched:
                    lock.release()
                    return get_result_previous(student, search_space - result_fetched)
                result_fetched.add(seat_number)
                lock.release()
                return True, fetch_details(res.text, seat_number)
            else:
                return get_result_previous(student, search_space - result_fetched)
    return False, student


def scrap_result(all_students, get_result_func, max_workers=30, max_retries=4):
    fetched_results = []
    _all_students = []  # list of student's whose result could not be fetched!
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = (
            executor.submit(get_result_func, student) for student in all_students
        )
        for future in as_completed(futures):
            success, student = future.result()
            if success:
                if not student.sgpa:
                    msg = '{} Backlog(s)'.format(len(student.backlogs))
                else:
                    msg = 'SGPA {}'.format(student.sgpa)
                sys.stdout.write(
                    '\nResult for {} fetched, {}\n'.format(student.name, msg)
                )
                fetched_results.append(student)
            elif success is None:
                _all_students.append(student)
            else:
                sys.stdout.write('\nCould not fetch result for {}\n'.format(student.name))
    if _all_students:
        if max_retries > 0:
            return fetched_results + scrap_result(
                _all_students, get_result_func, max_retries-1
            )
        else:
            for student in _all_students:
                sys.stdout.write('Could not fetch result for {}\n'.format(student.name))
    return fetched_results


def get_valid_info():
    filepath = input('\nEnter the path to the pdf file: ')
    try:
        all_students = get_student_data(filepath)
        if not all_students:
            raise Exception(
                'Could not find any student details in that pdf. Perhaps wrong pdf file?'
            )
    except FileNotFoundError:
        raise Exception('Please enter a valid path.')
    except PdfReadError:
        raise Exception('This doesn\'t seem to be a valid PDF file.')

    result_home_page = input('\nEnter the url of your result page: ')
    try:
        res = requests.get(result_home_page, timeout=7)
    except (ConnectionError, ConnectTimeout):
        raise Exception('Please make sure your internet is working.')
    if not res.status_code == 200:
        raise Exception('Please Enter a valid url.')

    mode = input('\nThis pdf is from current or previous year? (current/previous): ')
    if mode not in ('current', 'previous'):
        raise Exception('Try practicing typing current or previous a few times. :/')
    return all_students, result_home_page, mode, res


def scrape():
    global result_home_page, soup
    all_students, result_home_page, mode, home_pg_src = get_valid_info()
    soup = BeautifulSoup(home_pg_src.text, 'lxml')
    sys.stdout.write('\nSit back and relax this will take a while...\n')
    func_map = {'current': get_result_current, 'previous': get_result_previous}
    max_workers = 20 if mode == 'previous' else 30
    fetched_results = scrap_result(all_students, func_map[mode], max_workers=max_workers)

    if fetched_results:
        sort_by = input(
            '\nDo you want to sort the students by SGPA or Seat Number? (sgpa/seat_no): '
        )
        if sort_by not in ('sgpa', 'seat_no'):
            sort_by = 'seat_no'
        reverse = True if sort_by == 'sgpa' else False
        fetched_results = sort_students(fetched_results, by=sort_by, reverse=reverse)
        file_name = save_result(fetched_results)
        sys.stdout.write('\nAll the data is written into {}\n'.format(file_name))
        show_stats = input('\nOh by the way, Do you want to see some stats? (yes/no): ')
        if show_stats not in ('yes', 'no'):
            show_stats = 'no'
        if show_stats == 'yes':
            backlog_stats = Counter(
                chain(*(student.backlogs for student in fetched_results))
            )
            all_clear = sum(1 for student in fetched_results if student.sgpa)
            for subject in backlog_stats:
                sys.stdout.write('\n{} students have backlog in {}'.format(
                    backlog_stats[subject], subject)
                )
            sys.stdout.write('\n{} students are All Clear!\n\n'.format(all_clear))
    else:
        sys.stdout.write('\nCould not fetch any results. :/\n\n')


def main():
    try:
        scrape()
    except KeyboardInterrupt:
        sys.stdout.write('\nWait... Exiting Gracefully!\n\n')
    except Exception as error:
        sys.stdout.write('\nError: {}\n\n'.format(str(error)))
    finally:
        sys.exit()
