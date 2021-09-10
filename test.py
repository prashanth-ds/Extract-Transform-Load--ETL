import pandas as pd

data = pd.read_csv('developer_survey_2019/survey_results_public.csv')

print(data.groupby("JobSat").count())