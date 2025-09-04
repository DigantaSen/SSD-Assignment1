-- =========================================
-- Q1: Admission Analysis
-- Dataset1 is assumed to be loaded into `admissions` table
-- =========================================

USE ssd_a1;

-- Drop old table if exists (safe for re-import)
DROP TABLE IF EXISTS admissions;

-- Create admissions table to load dataset1
CREATE TABLE admissions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- Unique internal ID
  StudentID VARCHAR(32),                  -- Student identifier
  FirstName VARCHAR(100),
  LastName VARCHAR(100),
  Age TINYINT,                            -- Age of student
  Gender VARCHAR(16),                     -- Male / Female
  City VARCHAR(100),
  State VARCHAR(100),
  Email VARCHAR(255),
  PhoneNumber VARCHAR(32),
  Stage VARCHAR(100),                      -- Stage in admission process
  ExamDateTime DATETIME,                   -- Timestamp of stage completion
  Status VARCHAR(32),                      -- 'Pass' or 'Fail'
  INDEX idx_studentid (StudentID),         -- Index for faster student lookup
  INDEX idx_stage (Stage),                 -- Index for faster stage-based queries
  INDEX idx_city (City)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================
-- Q1a: Admission Funnel
-- This query calculates:
--   1. Number of students at each stage (funnel)
--   2. Average turnaround time (days) between consecutive stages
-- =========================================

WITH stage_times AS (
  SELECT
    StudentID,
    Stage,
    ExamDateTime,
    -- Get previous stage time per student to calculate turnaround
    LAG(ExamDateTime) OVER (
      PARTITION BY StudentID ORDER BY ExamDateTime
    ) AS prev_stage_time
  FROM admissions
),
per_stage AS (
  SELECT
    Stage,
    COUNT(DISTINCT StudentID) AS NumberOfStudents,  -- Count unique students per stage
    AVG(
      TIMESTAMPDIFF(SECOND, prev_stage_time, ExamDateTime) / 86400.0  -- Convert seconds to days
    ) AS AvgTurnaroundDays
  FROM stage_times
  GROUP BY Stage
)
SELECT Stage, NumberOfStudents, ROUND(AvgTurnaroundDays,2) AS AvgTurnaroundDays
FROM per_stage
-- Ensure logical ordering of stages in funnel
ORDER BY FIELD(Stage,
  'Technical Entrance Test',
  'IQ Test',
  'Descriptive Exam',
  'Face-to-Face Interview');

-- =========================================
-- Q1b: Pass and Fail Rate
-- This query calculates pass rate by multiple dimensions:
--   - Gender (Male/Female)
--   - Age Band (18-20, 21-23, 24-25)
--   - City
-- =========================================

SELECT
    Stage,
    Gender,
    CASE
        WHEN Age BETWEEN 18 AND 20 THEN '18-20'
        WHEN Age BETWEEN 21 AND 23 THEN '21-23'
        WHEN Age BETWEEN 24 AND 25 THEN '24-25'
        ELSE 'Other'
    END AS AgeBand,                               -- Group ages into bands
    City,
    ROUND(AVG(Status = 'Pass') * 100, 6) AS PassRate  -- Calculate pass rate percentage
FROM admissions
GROUP BY
    Stage,
    Gender,
    AgeBand,
    City
ORDER BY
    FIELD(Stage,
        'Technical Entrance Test',
        'IQ Test',
        'Descriptive Exam',
        'Face-to-Face Interview'),
    Gender,
    AgeBand,
    City;

-- =========================================
-- Q1c: Stored Procedure
-- Input: StudentID
-- Output:
--   - Student performance per stage
--   - Cohort pass rate by same stage, gender, city, age band
-- =========================================

DELIMITER $$

DROP PROCEDURE IF EXISTS sp_student_summary$$

CREATE PROCEDURE sp_student_summary(IN p_studentid VARCHAR(32))
BEGIN
    SELECT
        s.Stage,
        s.student_status AS YourStatus,       -- Student's own status
        c.CohortPassRate                      -- Average pass rate of other students in same cohort
    FROM (
        -- Student’s own details
        SELECT
            StudentID,
            Stage,
            Status AS student_status,
            Gender,
            City,
            CASE
                WHEN Age BETWEEN 18 AND 20 THEN '18-20'
                WHEN Age BETWEEN 21 AND 23 THEN '21-23'
                WHEN Age BETWEEN 24 AND 25 THEN '24-25'
                ELSE 'Other'
            END AS AgeBand
        FROM admissions
        WHERE StudentID = p_studentid
    ) s
    LEFT JOIN (
        -- Cohort stats excluding this student
        SELECT
            Stage,
            Gender,
            City,
            CASE
                WHEN Age BETWEEN 18 AND 20 THEN '18-20'
                WHEN Age BETWEEN 21 AND 23 THEN '21-23'
                WHEN Age BETWEEN 24 AND 25 THEN '24-25'
                ELSE 'Other'
            END AS AgeBand,
            ROUND(AVG(Status = 'Pass') * 100, 2) AS CohortPassRate
        FROM admissions
        WHERE StudentID <> p_studentid
        GROUP BY Stage, Gender, City, AgeBand
    ) c
    ON s.Stage = c.Stage
       AND s.Gender = c.Gender
       AND s.City = c.City
       AND s.AgeBand = c.AgeBand
    ORDER BY FIELD(s.Stage,
        'Technical Entrance Test',
        'IQ Test',
        'Descriptive Exam',
        'Face-to-Face Interview');
END$$

DELIMITER ;

-- Example call to procedure
CALL sp_student_summary('S202507954');
