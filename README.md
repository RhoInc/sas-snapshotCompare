# sas-snapshotCompare
Compare dataset, variable, and ID metadata with SAS

## Abstract
Little did you know that your last delivery ran on incomplete data.  To make matters worse, the client realized the issue first.  Sounds like a horror story, no?  A few preventative measures can go a long way in ensuring that your data are up-to-date and progressing normally.  At the data set level, metadata comparisons between the current and previous data cuts will help identify observation and variable discrepancies.  Comparisons will also uncover attribute differences at the variable level.  At the subject level they will identify missing subjects.  By compiling these comparison results into a comprehensive scheduled e-mail, a data facilitator need only skim the report to confirm that the data is good to go--or in need of some corrective action.  This paper introduces a suite of checks contained in a macro that will compare data cuts at the dataset, variable, and subject levels and produce an e-mail report.  The wide use of this macro will help all SAS® users create better deliveries while avoiding rework.