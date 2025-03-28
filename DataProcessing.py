import pandas as pd
from faker import Faker
import os
import random

# Initialize Faker for generating fake names
fake = Faker()

# Paths
raw_data_path = "./RawData/"
output_path = "./ProcessedData/"

# Ensure output directory exists
os.makedirs(output_path, exist_ok=True)

# Load studentInfo.csv
student_info = pd.read_csv(os.path.join(raw_data_path, "studentInfo.csv"))

# Generate unique names for each student ID
unique_students = student_info[['id_student']].drop_duplicates()
unique_students['student_name'] = [fake.name() for _ in range(len(unique_students))]

# Merge the names back into student_info
student_info = student_info.merge(unique_students, on="id_student", how="left")

# Create new table: students.csv (keeping only non-course related personal info)
students = student_info[["id_student", "student_name", "gender", "region", "highest_education", "imd_band", "age_band","disability","studied_credits"]]
students = students.drop_duplicates()  # Ensure no duplicate rows
students_path = os.path.join(output_path, "students.csv")
students.to_csv(students_path, index=False)

# Modify studentInfo.csv (remove personal details but keep id_student as FK)
student_info = student_info[["code_module", "code_presentation", "id_student", "num_of_prev_attempts", "final_result"]]
student_info.to_csv(os.path.join(output_path, "studentInfo.csv"), index=False)

'''--------For mental health survey data--------'''
# Load the mental health survey data
survey_file = os.path.join(raw_data_path, "students_mental_health_survey.csv")
survey_df = pd.read_csv(survey_file)

# Drop Age and Gender columns
survey_df = survey_df.drop(columns=['Age', 'Gender'], errors='ignore')

# Load the existing students.csv
students = pd.read_csv(students_path)

# Ensure the survey data gets `id_student` from existing students
num_existing_students = len(students)
num_survey_rows = len(survey_df)

if num_survey_rows <= num_existing_students:
    # Assign survey students an existing `id_student`
    survey_df["id_student"] = students["id_student"].sample(n=num_survey_rows, replace=False).values

# Select relevant columns for merging
survey_data = survey_df[["id_student", "Course", "CGPA"]].rename(columns={"Course": "course"})

# Merge with existing students.csv
students = students.merge(survey_data, on="id_student", how="left")

# Fill missing CGPA and Course values 
students["CGPA"] = students["CGPA"].apply(lambda x: x if pd.notna(x) else round(random.uniform(2.0, 4.0), 2))
valid_courses = survey_df["Course"].dropna().unique().tolist()
students["course"] = students["course"].apply(lambda x: x if pd.notna(x) else random.choice(valid_courses) if valid_courses else "Unknown")
# Replace "Others" with a random Arts course
arts_courses = ["Fine Arts", "Literature", "History", "Philosophy", "Music", "Theater", "Graphic Design"]
students["course"] = students["course"].apply(lambda x: random.choice(arts_courses) if str(x).strip().lower() == "others" else x)


survey_df = survey_df.drop(columns=['Course', 'CGPA'], errors='ignore')
students.to_csv(students_path, index=False)
survey_df.to_csv(os.path.join(output_path, "students_mental_health_survey.csv"), index=False)


'''--------For health  data--------'''
from datetime import datetime, timedelta

# Load the healthcare dataset
healthcare_file = os.path.join(raw_data_path, "healthcare_dataset.csv")
healthcare_df = pd.read_csv(healthcare_file)

# Drop the 'Name', 'Age', 'Gender' columns
healthcare_df = healthcare_df.drop(columns=['Name', 'Age', 'Gender','Hospital'], errors='ignore')
healthcare_df['Billing Amount'] = healthcare_df['Billing Amount'].apply(
    lambda x: x / 100 if x > 10000 else x
)
# Load the existing students.csv
students = pd.read_csv(students_path)

# Ensure the healthcare dataset gets `id_student` from existing students
num_existing_students = len(students)
num_healthcare_rows = len(healthcare_df)

if num_healthcare_rows <= num_existing_students:
    # Assign healthcare rows an existing `id_student`
    healthcare_df["id_student"] = students["id_student"].sample(n=num_healthcare_rows, replace=True).values

# Ensure 'Date of Admission' is within the last 6 years for each student
current_date = datetime.now()
six_years_ago = current_date - timedelta(days=6 * 365)

# Convert 'Date of Admission' to datetime format if it's not already
healthcare_df['Date of Admission'] = pd.to_datetime(healthcare_df['Date of Admission'], errors='coerce')

# Filter rows with 'Date of Admission' within the last 6 years
healthcare_df = healthcare_df[healthcare_df['Date of Admission'] >= six_years_ago]

# Save the updated healthcare dataset with the student_id column
healthcare_df.to_csv(os.path.join(output_path, "healthcare_dataset_updated.csv"), index=False)


print("saved in ./ProcessedData/")

