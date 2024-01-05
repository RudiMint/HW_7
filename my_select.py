from sqlalchemy import select, func, desc, and_

from HW_7.conf.models import Grade, Student, Subject, Teacher, Group
from HW_7.conf.db import session

def select_01():
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc("avg_grade")).limit(5).all()
    return result


def select_02():
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label("avg_grade")) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(desc("avg_grade")).limit(1).all()
    return result


def select_03():
    result = (
        session.query(Group.name.label('group_name'), func.avg(Grade.grade).label('average_grade'))
        .join(Student, Group.id == Student.group_id).join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id).filter(Subject.id == 1)
        .group_by(Group.id, Group.name).all()
    )
    return result


def select_04():
    result = (session.query(func.avg(Grade.grade).label('average_grade')).scalar())
    return result


def select_05():
    result = (
        session.query(Subject.name.label('course_name')).join(Teacher, Subject.teacher_id == Teacher.id) \
        .filter(Teacher.id == 1).all()
    )
    return result


def select_06():
    result = (
        session.query(Student.fullname.label('student_name')).join(Group, Student.group_id == Group.id)
        .filter(Group.id == 1).all()
    )
    return result


def select_07():
    result = (
        session.query(Student.fullname.label('student_name'), Grade.grade).join(Grade, Student.id == Grade.student_id)
        .join(Subject, Grade.subject_id == Subject.id).filter(Student.group_id == 1, Subject.id == 3).all()
    )
    return result


def select_08():
    result = (
        session.query(Teacher.fullname.label('teacher_name'),Subject.name.label('course_name'),func.avg(Grade.grade).label('average_grade'))
        .join(Subject, Teacher.id == Subject.teacher_id).join(Grade, Subject.id == Grade.subject_id)
        .filter(Teacher.id == 1).group_by(Teacher.fullname, Subject.name).all()
    )
    return result


def select_09():
    result = (
        session.query(Subject.name.label('course_name')).join(Grade, Subject.id == Grade.subject_id)
        .join(Student, Grade.student_id == Student.id).filter(Student.id == 1).distinct().all()
    )
    return result


def select_10():
    result = (
        session.query(Subject.name.label('course_name')).join(Grade, Subject.id == Grade.subject_id)
        .join(Student, Grade.student_id == Student.id).join(Teacher, Subject.teacher_id == Teacher.id)
        .filter(Student.id == 1, Teacher.id == 2).distinct().all()
    )
    return result


if __name__ == '__main__':
    print(select_01())
    print(select_02())
    print(select_03())
    print(select_04())
    print(select_05())
    print(select_06())
    print(select_07())
    print(select_08())
    print(select_09())
    print(select_10())
