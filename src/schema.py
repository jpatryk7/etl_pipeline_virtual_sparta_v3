from sqlalchemy.types import TEXT, INTEGER, DATE, BOOLEAN
from typing import Union


tables_list = [
    "student_information",
    "invitation",
    "test_score",
    "academy_performance",
    "trainee_performance",
    "tech_self_score",
    "weakness",
    "strength",
    "tech_self_score_junction",
    "weakness_junction",
    "strength_junction",
    "course",
    "trainer"
]

connection_list = [
    ("student_information", "invitation"),
    ("test_score", "student_information"),
    ("academy_performance", "student_information"),
    ("trainee_performance", "student_information"),
    ("tech_self_score_junction", "student_information"),
    ("tech_self_score_junction", "tech_self_score"),
    ("weakness_junction", "student_information"),
    ("weakness_junction", "weakness"),
    ("strength_junction", "student_information"),
    ("strength_junction", "strength"),
    ("academy_performance", "course"),
    ("course", "trainer"),
]

trainer_dtypes = {'index': INTEGER, 'trainer_name': TEXT}
trainer_col_names = [*trainer_dtypes]

course_dtypes = {'index': INTEGER, 'course_name': TEXT, 'trainer_id': INTEGER}
course_col_names = [*course_dtypes]

academy_performance_dtypes = {
    'index': INTEGER,
    'week': TEXT,
    'analytic': INTEGER,
    'independent': INTEGER,
    'determined': INTEGER,
    'professional': INTEGER,
    'studious': INTEGER,
    'imaginative': INTEGER,
    'student_information_id': INTEGER,
    'course_id': INTEGER
}
academy_performance_col_names = [*academy_performance_dtypes]

student_information_dtypes = {
    'index': INTEGER,
    'student_name': TEXT,
    'gender': TEXT,
    'dob': DATE,
    'email': TEXT,
    'city': TEXT,
    'address': TEXT,
    'postcode': TEXT,
    'phone_number': TEXT,
    'uni': TEXT,
    'degree': TEXT,
    'invitation_id': INTEGER
}
student_information_col_names = [*student_information_dtypes]

invitation_dtypes = {'index': INTEGER, 'invited_date': DATE, 'invited_by': TEXT}
invitation_col_names = [*invitation_dtypes]

trainee_performance_dtypes = {
    'index': INTEGER,
    'self_development': BOOLEAN,
    'geo_flex': BOOLEAN,
    'financial_support': BOOLEAN,
    'result': TEXT,
    'course_interest': TEXT,
    'student_information_id': INTEGER
}
trainee_performance_col_names = [*trainee_performance_dtypes]

weakness_junction_dtypes = {'student_information_id': INTEGER, 'weakness_id': INTEGER}
weakness_junction_col_names = [*weakness_junction_dtypes]

strength_junction_dtypes = {'student_information_id': INTEGER, 'strength_id': INTEGER}
strength_junction_col_names = [*strength_junction_dtypes]

tech_self_score_junction_dtypes = {'student_information_id': INTEGER, 'tech_self_score_id': INTEGER}
tech_self_score_junction_col_names = [*tech_self_score_junction_dtypes]

test_score_dtypes = {
    'index': INTEGER,
    'psychometrics': TEXT,
    'presentation': TEXT,
    'student_information_id': INTEGER
}
test_score_col_names = [*test_score_dtypes]

weakness_dtypes = {'index': INTEGER, 'weakness': TEXT}
weakness_col_names = [*weakness_dtypes]

strength_dtypes = {'index': INTEGER, 'strength': TEXT}
strength_col_names = [*strength_dtypes]

tech_self_score_dtypes = {'index': INTEGER, 'tech_self_score': TEXT}
tech_self_score_col_names = [*tech_self_score_dtypes]


def get_value(var_name: str) -> Union[list, dict]:
    try:
        return globals()[var_name]
    except KeyError:
        raise KeyError(f"Cannot identify {var_name} in Schema.")
