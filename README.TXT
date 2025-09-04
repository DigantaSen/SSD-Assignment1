README.TXT
===========

Assignment 1 – SQL and NoSQL
CS6.302 – Software System Development
Submission Date: 05 September 2025


--------------------------------------------------
Q1 (SQL Part)
--------------------------------------------------
Files:
- q1.sql
- q1_results.txt

Execution:
1. Load dataset1 into MySQL under database `ssd_a1`.
2. Run the script `q1.sql`. This script will:
   - Create the `admissions` table.
   - Insert dataset1 records (assumed to be preloaded).
   - Execute queries for:
       (a) Admission Funnel with average turnaround time.
       (b) Pass/Fail rate by gender, age band, and city.
       (c) Stored Procedure `sp_student_summary(StudentID)` which:
           - Accepts a StudentID as input.
           - Returns the student’s performance per stage.
           - Compares with cohort performance (gender, city, age band).
3. Query results are provided in `q1_results.txt`.


Assumptions:
- The stages follow a fixed logical order:
  [Technical Entrance Test → IQ Test → Descriptive Exam → Face-to-Face Interview].
- Age bands are grouped as: 18–20, 21–23, 24–25, Other.
- Average turnaround time is calculated in days between consecutive stages.
- Missing or null timestamps are excluded from turnaround calculations.


--------------------------------------------------
Q2 (NoSQL Part)
--------------------------------------------------
Files:
- q2a.js
- q2a_results.txt
- q2b.js
- q2b_results.txt

Execution:
1. Import dataset2 into MongoDB collection `temps`.
2. Run `q2a.js` using the `mongo` shell:
   mongo < q2a.js
   This script outputs:
     (a.1) Daily average temperature per city by month.
     (a.2) Monthly average temperature per city.
     (a.3) Hottest and coldest cities overall.
   Results are provided in `q2a_results.txt`.

3. Run `q2b.js` using the `mongo` shell:
   mongo < q2b.js
   This script outputs:
     (b.1) Top 5 hottest days nationwide.
     (b.2) Top 5 coldest days nationwide.
     (b.3) Rain check for Mumbai on 2025-06-15.
     (b.4) 7-day moving average for Delhi (last 10 days of June).
   Results are provided in `q2b_results.txt`.

Assumptions:
- Temperature values are stored under `temp.avg_c`.
- Precipitation is considered as `precip_mm` (default 0 if null).
- Rain is considered to have occurred if precipitation > 0.
- All dates are between Jan–Jun 2025 (per assignment spec).


--------------------------------------------------
Declaration
--------------------------------------------------
This submission was prepared individually. 

I used OpenAI ChatGPT (LLM) for limited assistance in this assignment. 
Specifically, I asked for:
- Clarification of SQL concepts (e.g., using window functions like LAG for calculating stage-wise turnaround time). 
- Guidance on MongoDB aggregation pipelines (grouping by city and month). 
- Help drafting a README file in a clear format. 

All code was written, verified, and tested by me. 
I did not copy-paste entire solutions; instead, I used the LLM to better understand concepts and debug errors.


All final code and results were verified locally before submission.

--------------------------------------------------
End of File
--------------------------------------------------

