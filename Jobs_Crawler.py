from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

import os
import time
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
from langdetect import detect

'''
Crawling of all Jobs in StackOverflow using Selenium.
'''


# Finds Number of Pages of Jobs in SO
def find_num_of_pages():
    pages=driver.find_element_by_class_name("s-pagination")
    numbers=pages.find_elements_by_css_selector("a>span")
    max_num_of_pages=2
    for n in numbers:
        if n.text.strip().isdigit():
            if int(n.text.strip())>max_num_of_pages:
                max_num_of_pages=int(n.text.strip())
    print("NUMBER OF PAGES: ", max_num_of_pages)
    return max_num_of_pages

# SET sorting to 'newest' programmatically
def start_from_newest():
    driver.find_element_by_xpath("//*[@class='grid--cell s-select']/select[@name='sort']/option[contains(text(),'newest')]").click()

def create_df(jobs):

    df=pd.DataFrame(columns=["Title","Company name","Location","Salary",
                  "Category","Visa","Relocation"
                  ,"About this job","Remote details","Technologies","Job description"
                ,"Skills-Requirements","Benefits","Likes","Dislikes","Love","Post date"])
    for i in jobs:
        df.loc[len(df)+1]=i
    
    # Crawling date : Datetime Timestamp
    # Consider Crawling date the Timestamp that crawl process ends for simplicity
    df["Crawling date"]=pd.to_datetime(datetime.now())
    return df



def preprocess_df(df):

    # Removing "–" from Location column & Removing heading and trailing spaces
    df["Location"]=df["Location"].replace("–","",regex=True)
    df["Location"]=df["Location"].str.strip()
    # If not Location given then Location='-' and after replace it becomes NaN
    df.loc[df["Location"].isna(),"Location"]="Undefined"

    # Reset Index (0 - len(df)-1)
    df.reset_index(drop=True,inplace=True)

    add_english_column(df)
    about_this_job_seperation(df)
    add_preferred_timezone(df)
    process_post_date(df)
    equity_salary_seperation(df)

    # DROP DUPLICATES
    # Based on subset as primary key
    # Doesn't include: Post Date, Crawling Date, Poste Date Timestamp, Likes , Dislikes, Love , Number Of applicants
    df.drop_duplicates(subset=['Title', 'Company name', 'Location', 'Salary', 'Category', 'Visa',
       'Relocation', 'Remote details', 'Technologies', 'Job description',
       'Skills-Requirements', 'Benefits', 'Language', 'Job Type',
       'Experience level', 'Role', 'Industry', 'Company size', 'Company type',
       'Preferred timezone', 'High response rate','Equity'],keep="first",inplace=True)
    # Reset Index (0 - len(df)-1)
    df.reset_index(drop=True,inplace=True)



# ENGLISH - NON ENGLISH
def add_english_column(df):
    # Better NOT iterrate through df ...
    for index, row in df.iterrows():
        lang=detect(df.loc[index,"Job description"])

        if lang!='en':
            df.loc[index,"Language"]='non-en'
        else:
            df.loc[index,"Language"]='en'

# About this job parts into Job Type,Experience Level , Role , Industry,Company size, Company type
def about_this_job_seperation(df):
    df["Job Type"]=df["About this job"].str.extract(r"Job type: ([^\n]*)")
    df.loc[df["Job Type"].isna(),"Job Type"]="Undefined"
    df["Experience level"]=df["About this job"].str.extract(r"Experience level: ([^\n]*)")
    df.loc[df["Experience level"].isna(),"Experience level"]="Undefined"
    df["Role"]=df["About this job"].str.extract(r"Role: ([^\n]*)")
    df.loc[df["Role"].isna(),"Role"]="Undefined"
    df["Industry"]=df["About this job"].str.extract(r"Industry: ([^\n]*)")
    df.loc[df["Industry"].isna(),"Industry"]="Undefined"
    df["Company size"]=df["About this job"].str.extract(r"Company size: ([^\n]*)")
    df.loc[df["Company size"].isna(),"Company size"]="Undefined"
    df["Company type"]=df["About this job"].str.extract(r"Company type: ([^\n]*)")
    df.loc[df["Company type"].isna(),"Company type"]="Undefined"
    df.drop(["About this job"],axis='columns',inplace=True)

def add_preferred_timezone(df):
    # Regex to extract Preffered Timezone GMT + flexibility in hours
    # (\(GMT.*\)) capture everything like (GMT ...)
    # (\+\/\-.*hours) capture everything like +/- ... hours
    df["Preferred timezone"]=df["Remote details"].str.extract(r"(?s)(\(GMT.*\))")
    df["Time Flexibility"]=df["Remote details"].str.extract(r"(?s)(\+\/\-.*hours)")
    df.loc[df["Preferred timezone"].isna(),"Preferred timezone"]="Undefined"
    df.loc[df["Time Flexibility"].isna(),"Time Flexibility"]=""
    
    df["Preferred timezone"]=df["Preferred timezone"]+" "+df["Time Flexibility"]
    df["Preferred timezone"]=df["Preferred timezone"].str.strip()
    
    df.drop(["Time Flexibility"],inplace=True, axis='columns')

# Get Post Date Timestamp , High response rate , Number of applicants now
def process_post_date(df):
    
    # High response rate
    df["High response rate"]="No"
    df.loc[df["Post date"].str.contains("High response rate",case=False),"High response rate"]="Yes"
    
    # Number of applicants
    #https://meta.stackoverflow.com/questions/345817/be-one-of-the-first-10-applicants-may-not-be-sending-the-right-message-to-job/345820
    df["Number of applicants"]="High"
    df.loc[df["Post date"].str.contains("Be one of the first applicants",case=False),"Number of applicants"]="Low"
    
    hours_dates=df.loc[df["Post date"].str.contains("hour",case=False),"Post date"]
    days_dates=df.loc[df["Post date"].str.contains(" day",case=False),"Post date"]
    yesterday_dates=df.loc[df["Post date"].str.contains("Yesterday",case=False),"Post date"]
    
    # Day of crawling
    crawling_date=df["Crawling date"][0]
    
    hours_dates=hours_dates.apply(lambda x : crawling_date-timedelta(hours=int("".join(filter(str.isdigit, x)))))
    days_dates=days_dates.apply(lambda x : crawling_date-timedelta(days=int("".join(filter(str.isdigit, x)))))
    yesterday_dates=yesterday_dates.apply(lambda x : crawling_date-timedelta(days=1))
    
    df.loc[days_dates.index,"Post date timestamp"]=days_dates
    df.loc[yesterday_dates.index,"Post date timestamp"]=yesterday_dates
    df.loc[hours_dates.index,"Post date timestamp"]=hours_dates

# Split Equity and salary into two columns
def equity_salary_seperation(df):
    df["Equity"]="No Equity"
    df.loc[df["Salary"].str.contains("Equity",case=False),"Equity"]="Equity"
    df["Salary"]=df["Salary"].str.replace("Equity","",case=False)
    df["Salary"]=df["Salary"].str.strip()
    df["Salary"]=df["Salary"].str.replace("|","",regex=True)
    df.loc[df["Salary"]=="","Salary"]="Undefined"



#chromedriver.exe should be in path folder
# SPLIT SCREEN FOR CORRECT EXCECUTION
PATH=os.getcwd()
#BASE_URL="https://stackoverflow.com/jobs"
#BASE_URL_PAGES="https://stackoverflow.com/jobs?pg={}"


# SORTED URLS , STARTING FROM NEWEST EVERY TIME
BASE_URL="https://stackoverflow.com/jobs?sort=p"
BASE_URL_PAGES="https://stackoverflow.com/jobs?sort=p&pg={}" 

#EXEC_PATH=os.path.join(PATH,"chromedriver.exe")


driver=webdriver.Chrome()
driver.get(BASE_URL)
PAGES=find_num_of_pages()


# List of jobs per page
jobs_list=driver.find_elements_by_xpath('//*[@data-jobid and @data-result-id and @data-beacon-url]')
# Selected job to get info from
selected_job=driver.find_element_by_xpath('//*[@class and @data-url and @data-beacon-url]')    


#List with words that might be included when asking for skills and requirements
skills_list=['skills','requirements','qualifications','must have','your profile','what you should know','what we expect from you']
jobs=[]
#starting from page 1
page=1

# ------- Actual crawling of pages ----------

while True:
    # load BASE_URL_PAGES except from Page 1.
    if page!=1:
        CUR_URL=(BASE_URL_PAGES).format(page)
        driver.get(CUR_URL)
        jobs_list=driver.find_elements_by_xpath('//*[@data-jobid and @data-result-id and @data-beacon-url]')
    

    for i in range(len(jobs_list)):
        job_dict={"Title":"Undefined","Company name":"Undefined","Location":"Undefined","Salary":"Undefined",
                  "Category":"Non Remote","Visa":"No visa sponsor","Relocation":"No paid relocation"
              ,"About this job":"Undefined",
              "Remote details":"Undefined","Technologies":"Undefined","Job description":"Undefined",
                  "Skills-Requirements":"Undefined","Benefits":"Undefined",
                 "Likes":"Undefined","Dislikes":"Undefined","Love":"Undefined","Post date":"Undefined"}
        time.sleep(2)
        # List of jobs per page
        jobs_list=driver.find_elements_by_xpath('//*[@data-jobid and @data-result-id and @data-beacon-url]')

        job = WebDriverWait(jobs_list[i], 10).until(
                 EC.presence_of_element_located((By.CSS_SELECTOR, ".mb4.fc-black-800.fs-body3>a"))
             )

        #time.sleep(2)
        driver.execute_script("arguments[0].click();", job)

        try:
            time.sleep(2)
            main=WebDriverWait(driver,10).until(
              EC.presence_of_element_located((By.XPATH, "//*[@class='snippet-hidden']")))
        except Exception:
            print("Not Found")
            driver.get(BASE_URL)
            continue

        title=main.find_element_by_xpath("//*[@class='fs-headline1 sticky:fs-body3 sticky:sm:fs-subheading t mb4 sticky:mb2']/a").text
        try:
            company=main.find_element_by_xpath("//*[@class='fc-black-700 mb4 sticky:mb0 sticky:mr8 fs-body2 sticky:fs-body1 sticky:sm:fs-caption']/a").text
            print(company)
        except NoSuchElementException:
            print("Company Name NOT FOUND")

        try:
            location=main.find_element_by_css_selector(".fc-black-500").text
            print(location)
        except NoSuchElementException:
            print("Location Name NOT FOUND")

        try:
            details=main.find_element_by_xpath("//*[@class='horizontal-list horizontal-list__lg fs-body1 fc-black-500 sticky:fold-up']")
            try:
                salary=details.find_element_by_class_name("fc-green-400").text
                print(salary)
            except NoSuchElementException:
                salary="Undefined"
            try:
                remote=details.find_element_by_class_name("fc-yellow-500").text
                print(remote)
            except NoSuchElementException:
                remote="Non Remote"
            try:
                visa=details.find_element_by_class_name("fc-red-300").text
                print(visa)
            except NoSuchElementException:
                visa="No visa sponsor"
            try:
                relocation=details.find_element_by_class_name("fc-powder-400").text
                print(relocation)
            except NoSuchElementException:
                relocation="No paid relocation"
        except NoSuchElementException:
            print("NO Details: ")
            salary="Undefined"
            remote="Non Remote"
            visa="No visa sponsor"
            relocation="No paid relocation"


        reactions=main.find_element_by_xpath("//*[@class='grid--cell fl-shrink0 pt8 mtn1 md:d-none sticky:fade-out']")
        likes=reactions.find_element_by_xpath("//span[@title='Like']").get_attribute('data-val')
        dislikes=reactions.find_element_by_xpath("//span[@title='Dislike']").get_attribute('data-val')
        love=reactions.find_element_by_xpath("//span[@title='Love']").get_attribute('data-val')


        job_dict["Title"]=title
        job_dict["Company name"]=company
        job_dict["Location"]=location
        job_dict["Salary"]=salary
        job_dict["Visa"]=visa
        job_dict["Category"]=remote
        job_dict["Relocation"]=relocation
        job_dict["Likes"]=likes
        job_dict["Dislikes"]=dislikes
        job_dict["Love"]=love

        posted=main.find_element_by_id("overview-items").find_element_by_class_name("mb24").text
        job_dict["Post date"]=posted


        overview=main.find_element_by_id("overview-items").find_elements_by_class_name("mb32")

        job_info_columns=["About this job","Remote details","Technologies","Job description"]
        for element in overview:
            try:
                name=element.find_element_by_css_selector("h2.fs-subheading.mb16.fc-dark").text.strip()
                if name not in job_info_columns: # Not in general info
                    print("NEW HEADER")
                elif name=="Technologies":
                    # Add the technologies tags with space-separated str 
                    tag_elements=element.find_elements_by_tag_name("a")
                    job_tags_space_seperated=""
                    for tag in tag_elements:
                        job_tags_space_seperated+=(" "+tag.text)
                    job_dict["Technologies"]=(job_tags_space_seperated.strip())
                elif name=="Job description":
                    job_dict[name]=element.text.replace(name,"").strip()

                    # Look for skills in Job Description
                    try:
                        y=element.find_element_by_xpath("//div/p/strong[contains (translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'requirements') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'skills') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'qualifications') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'must have') or contains (translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'your profile') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'what you should know') or contains (translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'what we expect from you')]/parent::p/following-sibling::ul")
                        job_dict["Skills-Requirements"]=y.text.strip()
                    except NoSuchElementException:
                        print("NO SKILLS & REQUIREMENTS IN JOB DESCRIPTION")
                else:
                    job_dict[name]=element.text.replace(name,"").strip()

                if job_dict["Skills-Requirements"]=="Undefined":
                        # Look for skills in another position
                        try:
                            if any(x in name.lower() for x in skills_list):
                                z=element.find_element_by_css_selector("div")
                                job_dict["Skills-Requirements"]=z.text.strip()

                        except NoSuchElementException:
                            print("NO SKILLS & REQUIREMENTS")
            except :
                print("IGNORED INFO")

        company_items=main.find_element_by_id("company-items")

        try:
            benefits=company_items.find_element_by_xpath("//*[@class='-benefits mb32']")
            job_dict["Benefits"]=benefits.text.replace("Benefits","").strip()
        except NoSuchElementException:
            print("No Benefits Item")


        #append a list with info of each job
        jobs.append(list(job_dict.values()))

        #driver.back()
        driver.execute_script("window.history.go(-1)")
    
    # move to next page
    page+=1
    if page>PAGES :
        print("Last Page reached")
        break

# Create Final Dataframe
df=create_df(jobs)
preprocess_df(df)
# Save to CSV
#Crawling Date (YY-MM-DD) = Name Of CSV
csv_name="{}.csv".format(df["Crawling date"][0].strftime("%Y-%m-%d"))
df.to_csv(csv_name)