-- =========================================
-- Q1: Admission Analysis
-- Dataset1 is assumed to be loaded into `admissions` table
-- =========================================

USE ssd_a1;

-- drop old table if it exists (safe when reimporting)
DROP TABLE IF EXISTS admissions;

CREATE TABLE admissions (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,   -- Internal unique ID
  StudentID VARCHAR(32) NOT NULL,         -- Student identifier (from dataset)
  FirstName VARCHAR(100),                 -- First name
  LastName VARCHAR(100),                  -- Last name
  Age TINYINT,                            -- Age of student
  Gender VARCHAR(16),                     -- Male / Female
  City VARCHAR(100),                      -- City
  State VARCHAR(100),                     -- State
  Email VARCHAR(255),                     -- Email address
  PhoneNumber VARCHAR(32),                -- Phone number (stored as string)
  Stage VARCHAR(100),                     -- Admission stage
  ExamDateTime DATETIME,                  -- Timestamp of stage completion
  Status VARCHAR(32)                      -- Pass / Fail
);

-- =========================================
-- Q1a: Admission Funnel
-- This query calculates:
--   1. Number of students at each stage (funnel)
--   2. Average turnaround time (days) between consecutive stages
-- =========================================

WITH StudentProgress AS (
    SELECT
        Stage,
        Status,
        ExamDateTime,
        -- This window function finds the timestamp of the next stage for each student.
        LEAD(ExamDateTime) OVER (PARTITION BY StudentID ORDER BY ExamDateTime) AS NextStageTime,
        -- This handles duplicates by numbering a student's attempts at the same stage, so we can filter for the latest one.
        ROW_NUMBER() OVER (PARTITION BY StudentID, Stage ORDER BY ExamDateTime DESC) AS AttemptNum
    FROM
        admissions
)
-- The final SELECT statement aggregates the prepared data to build the funnel.
SELECT
    Stage,
    COUNT(*) AS students_started,
    SUM(CASE WHEN Status = 'Pass' THEN 1 ELSE 0 END) AS students_advanced,
    SUM(CASE WHEN Status = 'Fail' THEN 1 ELSE 0 END) AS students_dropped_out,
    -- Calculate the average turnaround time in days, rounded to one decimal place.
    ROUND(AVG(TIMESTAMPDIFF(HOUR, ExamDateTime, NextStageTime) / 24.0), 1) AS avg_turnaround_days
FROM
    StudentProgress
WHERE
    AttemptNum = 1 -- This filter ensures we only count the latest attempt for each student-stage combination.
GROUP BY
    Stage
ORDER BY
    -- The FIELD function ensures the output is in a logical order, not alphabetical.
    FIELD(Stage,
        'Technical Entrance Test',
        'IQ Test',
        'Descriptive Exam',
        'Face-to-Face Interview'
    
    );

-- =========================================
-- Q1b: Pass and Fail Rate
-- This query calculates pass rate by multiple dimensions:
--   - Gender (Male/Female)
--   - Age Band (18-20, 21-23, 24-25)
--   - City
-- =========================================

SELECT 
    Stage,
    'Gender' AS Dimension,
    Gender AS Category,
    ROUND(AVG(CASE WHEN UPPER(Status) = 'PASS' THEN 1 ELSE 0 END) * 100, 2) AS PassRate
FROM admissions
GROUP BY Stage, Gender

UNION ALL

SELECT 
    Stage,
    'AgeBand' AS Dimension,
    CASE
        WHEN Age BETWEEN 18 AND 20 THEN '18-20'
        WHEN Age BETWEEN 21 AND 23 THEN '21-23'
        WHEN Age BETWEEN 24 AND 25 THEN '24-25'
        ELSE 'Other'
    END AS Category,
    ROUND(AVG(CASE WHEN UPPER(Status) = 'PASS' THEN 1 ELSE 0 END) * 100, 2) AS PassRate
FROM admissions
GROUP BY Stage, Category

UNION ALL

SELECT 
    Stage,
    'City' AS Dimension,
    City AS Category,
    ROUND(AVG(CASE WHEN UPPER(Status) = 'PASS' THEN 1 ELSE 0 END) * 100, 2) AS PassRate
FROM admissions
GROUP BY Stage, City

ORDER BY 
    FIELD(Stage,
        'Technical Entrance Test',
        'IQ Test',
        'Descriptive Exam',
        'Face-to-Face Interview'),
    Dimension,
    Category;

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
        c.PeerPassRate,                       -- Peer pass rate in same cohort
        (100 - c.PeerPassRate) AS PeerFailRate -- Peer fail rate
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
        -- Peer stats excluding this student
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
            ROUND(AVG(CASE WHEN UPPER(Status) = 'PASS' THEN 1 ELSE 0 END) * 100, 2) AS PeerPassRate
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

-- Example call
CALL sp_student_summary('S202507930');
