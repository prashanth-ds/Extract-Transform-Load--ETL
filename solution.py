# from pyspark.sql import SparkSession
# from pyspark.sql import functions as func
#
#
# spark = SparkSession.builder.master("local[*]").appName("StackOverflow").getOrCreate()
#
# SO_data = spark.read.format('csv').option('header', 'True').option('InferSchema', 'True').load('developer_survey_2019/survey_results_public.csv')
#
# SO_data.write.mode('overwrite').option("path", 'survey_results_public').saveAsTable("SoData")
#
# data = spark.createDataFrame(spark.sql("select * from SoData").collect())
#
# data.show()


import pandas as pd
from pycountry_convert import country_alpha2_to_continent_code, country_name_to_country_alpha2
import numpy as np

data = pd.read_csv('developer_survey_2019/survey_results_public.csv')

"""
1st Job

'''
    print(data.groupby('Age1stCode').count())
    - the above line says that there are even strings in this columns -> integers, older than 85, younger than 5
'''
print(data.groupby('Age1stCode').count())


data['Age1stCode'] = data['Age1stCode'].replace(['Older than 85', 'Younger than 5 years', 'NaN'], [85, 5, 0])
# data['Age1stCode'] = data['Age1stCode'].replace('Younger than 5 years', '5')

print(data['Age1stCode'])

# data['Age1stCode'].astype(int)

data['Age1stCode'] = pd.to_numeric(data['Age1stCode'])
# data['Age1stCode'] = data['Age1stCode'].dropna(inplace=True)
# data['Age1stCode'].dropna(inplace=True)

print(data['Age1stCode'])

first = data.loc[data['DevType'].str.contains('Developer') == True]

first.reset_index(inplace=True)

first = first['Age1stCode'].mean()

print("1st Job\n\n\n")
print(first)

"""

"""
# 2nd Job

respondents_each_country = data.groupby('Country').count().to_dict()['Respondent']

data['python_developers'] = data['LanguageWorkedWith'].str.contains('Python')

data = data[data['python_developers'] == True]

devs_each_country = data.groupby('Country').count()['python_developers'].to_dict()

print("\n\n2nd Job\n")
for key, value in devs_each_country.items():
    print(key + ": " + str(round((devs_each_country[key]/respondents_each_country[key]*100), 2)) + "%")
"""

# 3rd Job

"""
    axis : {0 or ‘index’, 1 or ‘columns’}, default 0

    0 or ‘index’: apply function to each column
    or ‘columns’: apply function to each rowaxis : {0 or ‘index’, 1 or ‘columns’}, default 0
    
    0 or ‘index’: apply function to each column
    or ‘columns’: apply function to each row
"""

continents = {
    'NA': 'North America',
    'SA': 'South America',
    'AS': 'Asia',
    'OC': 'Australia',
    'AF': 'Africa',
    'EU': 'Europe',
}

data.replace("Hong Kong (S.A.R.)", 'Hong Kong', inplace=True)
data.replace("Venezuela, Bolivarian Republic of...", 'Venezuela', inplace=True)
data.replace("The former Yugoslav Republic of Macedonia", 'North Macedonia', inplace=True)
data.replace("Congo, Republic of the...", 'Congo', inplace=True)
data.replace("Timor-Leste", 'East Timor', inplace=True) # East Timor alpha2 code is TL, it isn't available in the package pycountry_convert, so put it under other countries -> exception
data.replace("Republic of Korea", 'Korea, Republic of', inplace=True)
data.replace("Libyan Arab Jamahiriya", 'Libya', inplace=True)


def country_to_continent(country):

    global continents

    if country == 'Other Country (Not Listed Above)' or country == "East Timor":
        return "Other Country (Not Listed Above)"
    # print(country)
    return continents[country_alpha2_to_continent_code(country_name_to_country_alpha2(country))]


'''
data = data.loc[data['DevType'].str.contains('Developer') == True]

data['continent'] = data['Country'].map(country_to_continent)

devs_each_continent_python = data.groupby('continent').mean()['ConvertedComp'].to_dict()

print("\n\n3rd Job\n")
print(devs_each_continent_python)
'''


'''
# 4th Job


def sub_splitting(slashes):
    sub_split = slashes.split('/')
    for lang in sub_split:
        yield lang


def splitting(languages):
    for langs in languages:
        for each_split in langs.split(';'):
            if '/' in each_split:
                for slash_split in sub_splitting(each_split):
                    yield slash_split
            else:
                yield each_split


def desired_language(languages):
    programming_languages = {}
    for respondent in splitting(languages):
        if respondent not in programming_languages.keys():
            programming_languages[respondent] = 1
        else:
            programming_languages[respondent] += 1

    return programming_languages


desired_langs = data['LanguageDesireNextYear']
desired_langs.dropna(inplace=True)
desired_langs.to_list()
print(max(desired_language(desired_langs).items(), key=lambda x: x[1]))

print(desired_language(desired_langs))
'''


# 5th Job

def proper_clean(gender):
    if gender == 'Man':
        return "Man"
    elif gender == 'Woman':
        return "Woman"
    else:
        return "OTHERS"

'''
data = data.loc[data['Hobbyist'] == 'Yes']

# data['Gender'].replace(np.NaN, "OTHERS")
data['Gender'].dropna(inplace=True)

data['Gender'] = data['Gender'].map(proper_clean)


data['continent'] = data['Country'].map(country_to_continent)

output = data.groupby(['continent', 'Gender']).count()

print(output)
'''


# # 6th Job part 1


# data = data.loc[data['DevType'].str.contains('Developer') == True]
#
# data['Gender'].dropna(inplace=True)
# data['Gender'] = data['Gender'].map(proper_clean)
#
# data['continent'] = data['Country'].map(country_to_continent)
#
# job_sat = data.groupby(['continent', 'Gender', 'JobSat']).size()
#
# career_sat = data.groupby(['continent', 'Gender', 'CareerSat']).size()
#
# print(job_sat)
# print(career_sat)


# 6th Job part 1


def transform(satisfaction):
    if satisfaction == 'Slightly satisfied' or satisfaction == 'Very satisfied':
        return 'Satisfied'
    elif satisfaction == 'Slightly dissatisfied' or satisfaction == 'Very dissatisfied':
        return 'Dissatisfied'
    else:
        return "Neither"


data = data.loc[data['DevType'].str.contains('Developer') == True]

data['Gender'].dropna(inplace=True)
data['Gender'] = data['Gender'].map(proper_clean)

data['continent'] = data['Country'].map(country_to_continent)

data['JobSat'] = data['JobSat'].map(transform)
data['CareerSat'] = data['CareerSat'].map(transform)

job_sat = data.groupby(['continent', 'Gender', 'JobSat']).size().to_dict()

career_sat = data.groupby(['continent', 'Gender', 'CareerSat']).size().to_dict()

print(job_sat)
print(career_sat)
