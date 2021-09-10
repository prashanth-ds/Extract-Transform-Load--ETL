import pandas as pd
from pycountry_convert import country_alpha2_to_continent_code, country_name_to_country_alpha2
import json
import warnings

warnings.filterwarnings("ignore")


class ETL:

    def __init__(self):
        self.data = pd.read_csv('developer_survey_2019/survey_results_public.csv')
        self.continents = {
                            'NA': 'North America',
                            'SA': 'South America',
                            'AS': 'Asia',
                            'OC': 'Australia',
                            'AF': 'Africa',
                            'EU': 'Europe',
                          }

    def __country_to_continent(self, country):
        if country == 'Other Country (Not Listed Above)' or country == "East Timor":
            return "Other Country (Not Listed Above)"
        return self.continents[country_alpha2_to_continent_code(country_name_to_country_alpha2(country))]

    def __pre_processing(self, data):
        data.replace("Hong Kong (S.A.R.)", 'Hong Kong', inplace=True)
        data.replace("Venezuela, Bolivarian Republic of...", 'Venezuela', inplace=True)
        data.replace("The former Yugoslav Republic of Macedonia", 'North Macedonia', inplace=True)
        data.replace("Congo, Republic of the...", 'Congo', inplace=True)
        data.replace("Timor-Leste", 'East Timor',
                     inplace=True)  # East Timor alpha2 code is TL, it isn't available in the package pycountry_convert, so put it under other countries -> exception
        data.replace("Republic of Korea", 'Korea, Republic of', inplace=True)
        data.replace("Libyan Arab Jamahiriya", 'Libya', inplace=True)
        return data

    def __proper_clean(self, gender):
        # this function is used to transform the ambiguity that the data had.

        if gender == 'Man':
            return "Man"
        elif gender == 'Woman':
            return "Woman"
        else:
            return "OTHERS"

    def __satisfation_transform(self, satisfaction):
        if satisfaction == 'Slightly satisfied' or satisfaction == 'Very satisfied':
            return 'Satisfied'
        elif satisfaction == 'Slightly dissatisfied' or satisfaction == 'Very dissatisfied':
            return 'Dissatisfied'
        else:
            return "Neither"

    def first_job(self):
        """
            print(self.data.groupby('Age1stCode').count())
            - the above line says that there are actual strings in this columns -> integers, older than 85, younger than 5
        """

        def __operations(data):
            data['Age1stCode'] = data['Age1stCode'].replace(['Older than 85', 'Younger than 5 years', 'NaN'], [85, 5, 0])  # transform the string rows to integer
            data['Age1stCode'] = pd.to_numeric(data['Age1stCode'])  # cast them to integers

            developer = data.loc[data['DevType'].str.contains('Developer') == True]  # filter the dataset to only developers

            developer.reset_index(inplace=True)

            return developer['Age1stCode'].mean().round(2)

        # __operations(self.data).to_csv("Reports/First Job.csv")
        return __operations(self.data)

    def second_job(self):
        """
            Here we need to get the percentage of developers who know python in each country.
        """

        def __operations(data):
            respondents_each_country = data.groupby('Country').count().to_dict()['Respondent']  # get the total number of respondents from each country

            data['python_developers'] = data['LanguageWorkedWith'].str.contains('Python')  # create a new column where it creates boolean values whether a respondent is a python developer or not.

            data = data[data['python_developers'] == True]  # filter python developers only

            devs_each_country = data.groupby('Country').count()['python_developers'].to_dict()  # group by according to countries

            for key, value in devs_each_country.items():
                yield key, str(str(round((devs_each_country[key] / respondents_each_country[key] * 100), 2)) + "%")  # use generator to retrieve the percentage of python developers in each country

        output = {}
        for key_string, value_string in __operations(self.data):
            output[key_string] = value_string

        with open("Reports/Second Job.json", 'w') as file:
            json.dump(output, file)

        return output

    def third_job(self):
        """
            Here, get the average salaries of developers across continents
        """

        def __operations(data):

            data = self.__pre_processing(data)

            data = data.loc[data['DevType'].str.contains('Developer') == True]

            data['continent'] = data['Country'].map(self.__country_to_continent)  # add a new column indicating which continent a country belongs to.

            return data.groupby('continent').mean()['ConvertedComp'].to_dict()  # then group by according to the continents

        output = __operations(self.data)
        with open("Reports/Third Job.json", 'w') as file:
            json.dump(output, file)
        return output

    def fourth_job(self):
        """
            Here, get the most desirable programming language in the year 2020

            - Use generators concept to retrieve the count of all programming languages
        """

        def __operations(data):
            def __sub_splitting(slashes):
                # split the languages which are provided with a '/' using generators.

                sub_split = slashes.split('/')
                for lang in sub_split:
                    yield lang

            def __splitting(languages):
                # split each respondent's desirable languages using generators.

                for langs in languages:
                    for each_split in langs.split(';'):
                        if '/' in each_split:
                            for slash_split in __sub_splitting(each_split):
                                yield slash_split
                        else:
                            yield each_split

            def __desired_language(languages):
                # store and count the desirable programming languages using generators.

                programming_languages = {}
                for respondent in __splitting(languages):
                    if respondent not in programming_languages.keys():
                        programming_languages[respondent] = 1
                    else:
                        programming_languages[respondent] += 1

                return programming_languages

            desired_langs = data['LanguageDesireNextYear']
            desired_langs.dropna(inplace=True)
            desired_langs.to_list()

            return max(__desired_language(desired_langs).items(), key=lambda x: x[1])

        output = __operations(self.data)
        with open("Reports/Fourth Job.json", 'w') as file:
            json.dump(output, file)
        return output

    def fifth_job(self):
        """
            Here, get the respondent's who love to code as a hobby based on their gender and continent.
        """

        def __operations(data):
            data = self.__pre_processing(self.data)

            data = data.loc[data['Hobbyist'] == 'Yes']  # filter all the respondent's who code as hobby.

            # data['Gender'].replace(np.NaN, "OTHERS")
            data['Gender'].dropna(inplace=True)

            data['Gender'] = data['Gender'].map(self.__proper_clean)

            data['continent'] = data['Country'].map(self.__country_to_continent)

            # return data.groupby(['continent', 'Gender']).size()
            return data.groupby(['continent', 'Gender']).size().to_dict()

        output = __operations(self.data)
        output = {value:list(key) for key, value in output.items()}
        with open("Reports/Fifth Job.json", 'w') as file:
            json.dump(output, file)
        return output

    def sixth_job1(self):
        """
            Here, we get the job and career satisfaction without transforming the satisfactions w.r.t continent and gender.
        """

        def __operations(data):
            data = data.loc[data['DevType'].str.contains('Developer') == True]  # only for developers

            data['Gender'].dropna(inplace=True)
            data['Gender'] = data['Gender'].map(self.__proper_clean)

            data['continent'] = data['Country'].map(self.__country_to_continent)

            job_sat = data.groupby(['continent', 'Gender', 'JobSat']).size()  # get the job satisfaction w.r.t to continent and gender.

            career_sat = data.groupby(['continent', 'Gender', 'CareerSat']).size()  # get the career satisfaction w.r.t to continent and gender.

            # if you want to see the complete dataframe, then uncomment the two lines below

            # job_sat.to_csv("job_sat.csv")
            # career_sat.to_csv("career_sat.csv")

            return {"Job Satisfaction": job_sat.to_dict(), "Career Satisfaction": career_sat.to_dict()}

        output = __operations(self.data)
        # print(output)
        # with open("Reports/Sixth Job.json", 'w') as file:
        #     json.dump(output, file)
        return output

    def sixth_job2(self):
        """
            Here, we get the job and career satisfaction without transforming the satisfactions w.r.t continent and gender.
        """

        def __operations(data):
            data = data.loc[data['DevType'].str.contains('Developer') == True]  # only for developers

            data['Gender'].dropna(inplace=True)
            data['Gender'] = data['Gender'].map(self.__proper_clean)

            data['continent'] = data['Country'].map(self.__country_to_continent)

            # the below two lines of code is to transform the satisfaction criteria.
            data['JobSat'] = data['JobSat'].map(self.__satisfation_transform)
            data['CareerSat'] = data['CareerSat'].map(self.__satisfation_transform)

            job_sat = data.groupby(
                ['continent', 'Gender', 'JobSat']).size()  # get the job satisfaction w.r.t to continent and gender.

            career_sat = data.groupby(['continent', 'Gender',
                                       'CareerSat']).size()  # get the career satisfaction w.r.t to continent and gender

            # if you want to see the complete dataframe, then uncomment the two lines below

            # job_sat.to_csv("job_sat.csv")
            # career_sat.to_csv("career_sat.csv")

            return {"Job Satisfaction": job_sat.to_dict(), "Career Satisfaction": career_sat.to_dict()}

        output = __operations(self.data)
        # with open("Reports/Sixth2 Job.json", 'w') as file:
        #     json.dump(output, file)
        return output


obj = ETL()

print("1st Job")
print(obj.first_job())

print("\n2nd Job")
print(obj.second_job())

print("\n3rd Job")
print(obj.third_job())

print("\n4th Job")
print(obj.fourth_job())

print("\n5th Job")
print(obj.fifth_job())

print("\n6th Job")
print(obj.sixth_job1())

print("\n6th Job (Alternative)")
print(obj.sixth_job2())
