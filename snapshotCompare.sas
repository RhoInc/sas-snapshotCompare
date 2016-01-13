/* ---------------------- Copyright 2015, Rho, Inc.  All rights reserved. ---------------------- */

/*-------------------------------------------------------------------------------------------------

    Program:      snapshotCompare

      Purpose:    Compare dataset, variable, and ID metadata between two snapshots or directories

        Examples:

          [Fully-qualified Path1 and Path2]

            %snapshotCompare
                (Path1   = C:\Project\Data\NewSnapshot
                ,Emails  = jane_doe@email.com john_doe@email.com
                ,Path2   = C:\Project\Data\OldSnapshot
                ,IDVar   = ID
                ,Detail  = ALL
                ,Project = Example Project);

          [Fully-qualified Path1 and partially-qualified Path2 (see logic in Params section)]

            %snapshotCompare
                (Path1   = C:\Project\Data
                ,Emails  = jane_doe@email.com john_doe@email.com
                ,Path2   = Old
                ,IDVar   = ID
                ,Detail  = ALL
                ,Project = Example Project);

          [Fully-qualified Path1 contains all snapshots in individual subfolders (see logic in Params section)]

            %snapshotCompare(Path1   = C:\Project\Data,
                             Emails  = jane_doe@email.com john_doe@email.com,
                             IDVar   = ID,
                             Detail  = ALL,
                             Project = Example Project);

        Parameters:

          [REQUIRED]

            Path1   - Directory of current snapshot or directory with all snapshots

            Emails  - List of pertinent email addresses

          [optional]

            Path2   - Directory of previous snapshot or folder name of previous
                      snapshot within &Path1, e.g. 'Old'

                1.  If Path2 is fully-qualified, e.g.
                    C:\Project\Data\Old, snapshotCompare first looks in Path2
                    for datasets.
                    a.  If Path2 contains datasets, snapshotCompare
                        compares those with datasets in Path1.
                    b.  If Path2 does not contain datasets,
                        snapshotCompare captures the most recently
                        created folder in Path2 and compares its
                        datasets with those in Path1.

                2.  If Path2 is not fully-qualified, e.g. 'Old',
                    snapshotCompare first looks in Path1\Path2 for
                    datasets.
                    a.  If Path1\Path2 contains datasets,
                        snapshotCompare compares those with datasets in
                        Path1.
                    b.  If Path1\Path2 does not contain datasets,
                        snapshotCompare captures the most recently
                        created folder in Path1\Path2 and compares its
                        datasets with those in Path1.

                3.  If Path2 is unspecified, snapshotCompare counts the
                    number of folders in Path1.
                    a.  If no folders exist, execution terminates.
                    b.  If one folder exists, snapshotCompare checks if
                        the lone folder contains datasets.
                        i.  If so, it compares datasets in the lone
                            folder with those in Path1.
                        ii. If not, snapshotCompare determines the most
                            recently created folder in the lone folder,
                            assuming the lone folder contains all
                            previous snapshots.
                    c.  If more than one folder exists, snapshotCompare
                        first checks if Path1 contains datasets.
                        i.  If so, it compares datasets in the most
                            recently created folder to those in Path1.
                        ii. If not, snapshotCompare compares datasets
                            in the most recently created folder in
                            Path1 to those in the next most recently
                            created folder.

            IDVar   - ID variable
                        + Defaults to ID

            Detail  - Detail level, a space-delimited list of the below values
                        + Defaults to ALL
                            > ALL generates the dataset-, variable-, and
                               ID-level reports.
                            > DS/DATA(SET) generates the dataset-level report.
                            > VAR(IABLE) generates the variable-level report.
                            > ID/SUBJECT generates the ID-level report.

            Project - Project name which appears on the email subject line

      Output:     An email report with subject line &Project Snapshot Comparison - <today's date>

  Program History:

      Date        Programmer            Description
      ----------  --------------------  ---------------------------------------------------------
      2015-03-26  Spencer Childress     Create

-------------------------------------------------------------------------------------------------*/

%macro snapshotCompare
    (Path1   = 
    ,Emails  = 
    ,Path2   = 
    ,IDVar   = ID
    ,Detail  = ALL
    ,Project = 
    ) / minoperator;

  %*Declare local macro variables.;
    %local xwait_ notes_ linesize_ DSRegex VarRegex IDRegex IDVarFN Path1Datasets Path2Datasets
           NewDatasets MissingDatasets ID1Datasets ID2Datasets Path1Directories Path2Directories
           CommonDatasets Hyperlink1 Hyperlink2;

  %*Capture option settings to return their value at macro termination.;
    %let xwait_    = %sysfunc(getoption(xwait));
    %let notes_    = %sysfunc(getoption(notes));
    %let threads_  = %sysfunc(getoption(threads));
    %let compress_ = %sysfunc(getoption(compress));
    %let linesize_ = %sysfunc(getoption(linesize));

    options noxwait nonotes threads compress = char linesize = 150;

  %*Define regular expressions for Detail parameter.;
    %let DSRegex  = (all|ds|data(set)?);
    %let VarRegex = (all|var(iable)?);
    %let IDRegex  = (all|id|subject);

  %*Define current date.;
    %let Today = %sysfunc(today(), yymmdd10.);

    %put;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put --> *    snapshotCompare BEGINNING EXECUTION                                                      *;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put;

        /*---------------------------------------------------------------------------------------*/
        /* Parameter error checking messages                                                     */
        /*---------------------------------------------------------------------------------------*/

            /*-----------------------------------------------------------------------------------*/
            /* Path1                                                                             */
            /*-----------------------------------------------------------------------------------*/

              %*Path1 unspecified.;
                %if %nrbquote(&Path1) = %then %do;
                    %put %str(    --> Path1 parameter unspecified.);
                    %put %str(    -->     Execution terminating.);

                    %goto exit;
                %end;
              %*Path1 nonexistent.;
                %else %if ^%sysfunc(fileexist(%nrbquote(&Path1))) %then %do;
                    %put %str(    --> &Path1 does not exist.);
                    %put %str(    -->     Execution terminating.);

                    %goto exit;
                %end;

            /*-----------------------------------------------------------------------------------*/
            /* Emails                                                                            */
            /*-----------------------------------------------------------------------------------*/

              %*Emails unspecified.;
                %if %nrbquote(&Emails) = %then %do;
                    %put %str(    --> Emails parameter unspecified.);
                    %put %str(    -->     Execution terminating.);

                    %goto exit;
                %end;
              %*Emails contains invalid email addresses.;
                %else %do i = 1 %to %sysfunc(countw(%nrbquote(&Emails), %str( )));
                    %let Email = %scan(%nrbquote(&Emails), &i, %str( ));

                    %if ^%sysfunc(prxmatch(%str(/^(?<!\.)[A-Z0-9._%+-]+(?<!\.)\@[A-Z0-9.-]+\.[A-Z]{2,4}$/i), %nrbquote(&Email))) %then %do;
                        %put %str(    --> &Email is not a valid email address.);
                        %put %str(    -->     Execution terminating.);

                        %goto exit;
                    %end;
                %end;

            /*-----------------------------------------------------------------------------------*/
            /* Path2                                                                             */
            /*-----------------------------------------------------------------------------------*/

                /*-------------------------------------------------------------------------------*/
                /* Path2 unspecified.                                                           */
                /*-------------------------------------------------------------------------------*/

                    %if %nrbquote(&Path2) = %then %do;
                        %put %str(    --> Path2 parameter unspecified. Attempting to locate previous snapshot.);

                      %*Open Path1 to determine directory setup.;
                        filename dir1list pipe %unquote(%bquote(')dir /a "&Path1\*"%bquote('));
                            data directories;
                                infile dir1list pad lrecl = 255;
                                input @1 fileinfo $255.;

                                format datetime datetime19. date date9. time time5.;

                              %*Look only at subdirectories.;
                                if prxmatch('/<DIR>(?! +\.+ *$)/', fileinfo) then do;
                                    datetimec = scan(fileinfo, 1, '<');
                                    date      = input(scan(datetimec, 1, ' '), mmddyy10.);
                                        time  = input(scan(datetimec, 2, ' '), time5.) +
                                                  ifn(scan(datetimec, -1) = 'PM' and
                                                        scan(scan(datetimec, 2, ' '), 1, ':') ne '12',
                                                          12*60*60,
                                                            0);
                                    datetime  = dhms(date, 0, 0, time);

                                    output;
                                end;
                            run;
                        filename dir1list clear;

                      %*Count number of folders in Path1.;
                        proc sql noprint;
                            select  count(1)
                              into :Path1Directories
                                from directories;
                        quit;

                        /*-----------------------------------------------------------------------*/
                        /* No subfolders exist in Path1.                                         */
                        /*-----------------------------------------------------------------------*/

                            %if       &Path1Directories = 0 %then %do;
                                %put %str(    --> No folders present in &Path1..);
                                %put %str(    -->     Execution terminating.);

                                %goto exit;
                            %end;

                        /*-----------------------------------------------------------------------*/
                        /* One subfolder exists in Path1.                                        */
                        /*-----------------------------------------------------------------------*/

                            %else %if &Path1Directories = 1 %then %do;
                                %put %str(    --> One folder present in &Path1..);

                              %*Capture fully-qualified subfolder in a macro variable.;
                                proc sql noprint;
                                    select  catx('\', "&Path1", scan(fileinfo, -1, '>'))
                                      into :Path2 separated by ' '
                                        from directories;
                                quit;

                                %put %str(    --> Checking &Path2 for datasets.);

                              %*Check if datasets exist in subfolder.;
                                libname _path2_ "&Path2" access = readonly;
                                    proc sql noprint;
                                        select  count(distinct 1)
                                          into :Path2Datasets separated by ' '
                                            from dictionary.tables
                                              where libname = '_PATH2_';
                                    quit;
                                libname _path2_ clear;

                            /*-------------------------------------------------------------------*/
                            /* Datasets exist in Path1\<subfolder>                               */
                            /*-------------------------------------------------------------------*/

                                %if       &Path2Datasets = 1 %then %put %str(    --> Datasets found in &Path2..);

                            /*-------------------------------------------------------------------*/
                            /* No datasets exist in Path1\<subfolder>                            */
                            /*-------------------------------------------------------------------*/

                                %else %do;
                                    %put %str(    --> No datasets present in &Path2..);
                                    %put %str(    -->     Checking for subfolders.);

                                  %*Open Path1\<subfolder> to determine directory setup.;
                                    filename dir2list pipe "dir /a &Path2\*";
                                        data subdirectories;
                                            infile dir2list pad lrecl = 100;
                                            input @1 fileinfo $255.;

                                            format datetime datetime19. date date9. time time5.;

                                            if prxmatch('/<DIR>(?! +\.+ *$)/', fileinfo) then do;
                                                datetimec = scan(fileinfo, 1, '<');
                                                date      = input(scan(datetimec, 1, ' '), mmddyy10.);
                                                    time  = input(scan(datetimec, 2, ' '), time5.) +
                                                              ifn(scan(datetimec, -1) = 'PM' and
                                                                    scan(scan(datetimec, 2, ' '), 1, ':') ne '12',
                                                                      12*60*60,
                                                                        0);
                                                datetime  = dhms(date, 0, 0, time);

                                                output;
                                            end;
                                        run;
                                    filename dir2list clear;

                                  %*Count number of folders in Path1\<subfolder>.;
                                    proc sql noprint;
                                        select  count(1)
                                          into :Path2Directories
                                            from subdirectories;
                                    quit;

                                /*---------------------------------------------------------------*/
                                /* No folders exist in Path1\<subfolder>                         */
                                /*---------------------------------------------------------------*/

                                    %if &Path2Directories = 0 %then %do;
                                        %put %str(    --> No folders present in &Path2..);
                                        %put %str(    -->     Execution terminating.);

                                        %goto exit;
                                    %end;

                                /*---------------------------------------------------------------*/
                                /* Folders exist in Path1\<subfolder>                            */
                                /*---------------------------------------------------------------*/

                                    %else %do;
                                      %*Determine most recently create folder in Path1/<subfolder>.;
                                        proc sort data = subdirectories;
                                            by descending datetime;
                                        run;

                                        data _null_;
                                            set subdirectories;

                                             if _n_ = 1 then call symputx('Path2', catx('\', "&Path2", scan(fileinfo, -1, '>')));
                                        run;
                                    %end;
                                %end;
                            %end;

                        /*---------------------------------------------------------------------------*/
                        /* Multiple subfolders exist in Path1.                                       */
                        /*---------------------------------------------------------------------------*/

                            %else %do;
                                %put %str(    --> Multiple folders present in &Path1..);

                                proc sql noprint;
                                    select  catx('\', "&Path1", scan(fileinfo, -1, '>'))
                                      into :Path2 separated by ' '
                                        from directories;
                                quit;

                                %put %str(    --> Checking &Path1 for datasets.);

                              %*Check if datasets exist in Path1.;
                                libname _path1_ "&Path1" access = readonly;
                                    proc sql noprint;
                                        select  count(distinct 1)
                                          into :Path1Datasets separated by ' '
                                            from dictionary.tables
                                              where libname = '_PATH1_';
                                    quit;
                                libname _path1_ clear;

                            /*-------------------------------------------------------------------*/
                            /* Datasets exist in Path1.                                          */
                            /*-------------------------------------------------------------------*/

                                %if &Path1Datasets = 1 %then %do;
                                    %put %str(    --> Datasets found in &Path1..);
                                    %put %str(    -->     Determining most recently created folder.);

                                  %*Determine most recently created folder in Path1.;
                                    proc sort data = directories;
                                        by descending datetime;
                                    run;

                                    data _null_;
                                        set directories;

                                        if _n_ = 1 then call symputx('Path2', catx('\', "&Path1", scan(fileinfo, -1, '>')));
                                    run;
                                %end;

                            /*-------------------------------------------------------------------*/
                            /* No datasets exist in Path1.                                       */
                            /*-------------------------------------------------------------------*/

                                %else %do;
                                  %*Determine two most recently created folders in Path1.;
                                    proc sort data = directories;
                                        by descending datetime;
                                    run;

                                    data _null_;
                                        set directories;

                                             if _n_ = 1 then call symputx('Path1', catx('\', "&Path1", scan(fileinfo, -1, '>')));
                                        else if _n_ = 2 then call symputx('Path2', catx('\', "&Path1", scan(fileinfo, -1, '>')));
                                    run;
                                %end;
                            %end;
                        %end;

                /*---------------------------------------------------------------------------*/
                /* Path2 is not fully-qualified                                              */
                /*---------------------------------------------------------------------------*/

                  %*Path2 does not begin with a letter and a colon.;
                    %else %if ^%sysfunc(prxmatch(/[a-z]:/i, %nrbquote(&Path2))) %then %do;
                        %put %str(    --> &Path2 is not fully-qualified.);

                        %let Path2 = &Path1\&Path2;

                      %*Path1\Path2 does not exist.;
                        %if ^%sysfunc(fileexist(%nrbquote(&Path2))) %then %do;
                            %put %str(    --> &Path2 does not exist.);
                            %put %str(    -->     Execution terminating.);

                            %goto exit;
                        %end;

                        %put %str(    --> Checking &Path2 for datasets.);

                      %*Check if datasets exist in Path1\Path2.;
                        libname _path2_ "&Path2" access = readonly;
                            proc sql noprint;
                                select  count(distinct 1)
                                  into :Path2Datasets separated by ' '
                                    from dictionary.tables
                                      where libname = '_PATH2_';
                            quit;
                        libname _path2_ clear;

                    /*---------------------------------------------------------------------------*/
                    /* Path1\Path2 contains datasets.                                            */
                    /*---------------------------------------------------------------------------*/

                        %if &Path2Datasets = 1 %then %put %str(    --> &Path2 contains datasets.);

                    /*---------------------------------------------------------------------------*/
                    /* Path1\Path2 does not contain datasets.                                    */
                    /*---------------------------------------------------------------------------*/

                        %else %do;
                            %put %str(    --> No datasets present in &Path2..);
                            %put %str(    -->     Checking for subfolders.);

                          %*Open Path1\Path2 to determine directory setup.;
                            filename dir2list pipe "dir /a &Path2\*";
                                data subdirectories;
                                    infile dir2list pad lrecl = 100;
                                    input @1 fileinfo $255.;

                                    format datetime datetime19. date date9. time time5.;

                                    if prxmatch('/<DIR>(?! +\.+ *$)/', fileinfo) then do;
                                        datetimec = scan(fileinfo, 1, '<');
                                        date      = input(scan(datetimec, 1, ' '), mmddyy10.);
                                            time  = input(scan(datetimec, 2, ' '), time5.) +
                                                      ifn(scan(datetimec, -1) = 'PM' and
                                                            scan(scan(datetimec, 2, ' '), 1, ':') ne '12',
                                                              12*60*60,
                                                                0);
                                        datetime  = dhms(date, 0, 0, time);

                                        output;
                                    end;
                                run;
                            filename dir2list clear;

                            proc sql noprint;
                                select  count(1)
                                  into :Path2Directories
                                    from subdirectories;
                            quit;

                        /*-----------------------------------------------------------------------*/
                        /* No folders exist in Path1\Path2.                                      */
                        /*-----------------------------------------------------------------------*/

                            %if &Path2Directories = 0 %then %do;
                                %put %str(    --> No folders present in &Path2.. Execution terminating.);

                                %goto exit;
                            %end;

                        /*-----------------------------------------------------------------------*/
                        /* Folders exist in Path1\Path2.                                         */
                        /*-----------------------------------------------------------------------*/

                            %else %do;
                                proc sort data = subdirectories;
                                    by descending datetime;
                                run;

                                data _null_;
                                    set subdirectories;

                                     if _n_ = 1 then call symputx('Path2', catx('\', "&Path2", scan(fileinfo, -1, '>')));
                                run;
                            %end;
                        %end;
                    %end;

                /*---------------------------------------------------------------------------*/
                /* Path2 is fully-qualified and nonexistent.                                 */
                /*---------------------------------------------------------------------------*/

                    %else %if ^%sysfunc(fileexist(%nrbquote(&Path2))) %then %do;
                        %put %str(    --> &Path2 does not exist. Execution terminating.);

                        %goto exit;
                    %end;

            /*-----------------------------------------------------------------------------------*/
            /* IDVar                                                                             */
            /*-----------------------------------------------------------------------------------*/

              %*IDVar unspecified.;
                %if %nrbquote(&IDVar) = %then %do;
                    %put %str(    --> IDVar parameter unspecified. ID-level report will not be generated.);

                    %let IDVarFN = 1;
                %end;
              %*IDVar is invalid.;
                %else %if ^%sysfunc(prxmatch(/^[_a-z][0-9_a-z]*$/i, %nrbquote(&IDVar))) %then %do;
                    %put %str(    --> &IDVar is an invalid variable name. ID-level report will not be generated.);

                    %let IDVarFN = 1;
                %end;
              %*IDVar specified and valid.;
                %else %do;
                    %let IDVar   = %upcase(&IDVar);
                    %let IDVarFN = 0;
                %end;

            /*-----------------------------------------------------------------------------------*/
            /* Detail                                                                            */
            /*-----------------------------------------------------------------------------------*/

              %*Detail unspecified.;
                %if %nrbquote(&Detail) = %then %do;
                    %if IDVarFN = 0 %then %do;
                        %put %str(    --> Detail parameter unspecified. Defaulting to ALL.);

                        %let Detail = ALL;
                    %end;
                    %else %do;
                        %put %str(    --> Detail parameter unspecified. Defaulting to DATASET VARIABLE.);

                        %let Detail = DATASET VARIABLE;
                    %end;
                %end;
              %*Detail contains an invalid value.;
                %else %if %nrbquote(&Detail) ne %then %do i = 1 %to %sysfunc(countw(%nrbquote(&Detail, %str( ))));
                    %let Parse = %upcase(%scan(%nrbquote(&Detail), &i, %str( )));

                    %if not %eval(%nrbquote(&Parse) in ALL DS DATA DATASET VAR VARIABLE ID SUBJECT) %then %do;
                        %put %str(    --> &Parse is an invalid value.);

                        %if IDVarFN = 0 %then %do;
                            %put %str(    --> Detail parameter defaulting to ALL.);

                            %let Detail = ALL;
                        %end;
                        %else %do;
                            %put %str(    --> Detail parameter defaulting to DATASET VARIABLE.);

                            %let Detail = DATASET VARIABLE;
                        %end;

                        %goto Detailed;
                    %end;

                    %Detailed:
                %end;
                %else %let Detail = %upcase(&Detail);

            /*-----------------------------------------------------------------------------------*/
            /* Project                                                                           */
            /*-----------------------------------------------------------------------------------*/

              %*Project unspecified.;
                %if %nrbquote(&Project) = %then %put %nrbquote(    --> Project parameter unspecified. Email subject line defaulting to 'Snapshot Comparison - &Today'.);

    %put;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put --> *    SNAPSHOT COMPARISON COMMENCING                                                           *;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put;

        %put %str(    --> Comparing contents of &Path1);
        %put %str(    -->     with contents of &Path2..);

        libname _Path1_ "&Path1" access = readonly;
        libname _Path2_ "&Path2" access = readonly;

        proc sql;
          %*&Path1 datasets;
            create table Path1Datasets as
                select *
                    from dictionary.tables
                      where libname = '_PATH1_';
            %let Path1Nobs = &SQLobs;
          %*&Path2 datasets;
            create table Path2Datasets as
                select *
                    from dictionary.tables
                      where libname = '_PATH2_';
            %let Path2Nobs = &SQLObs;
        quit;

      %*Terminate execution if either directory does not contain datasets.;
        %if &Path1Nobs = 0 or &Path2Nobs = 0 %then %do;
                  %if &Path1Nobs = 0 and &Path2Nobs = 0 %then %put %str(    --> No datasets found in either directory.);
            %else %if &Path1Nobs = 0                    %then %put %str(    --> No datasets found in &Path1..);
                                                        %else %put %str(    --> No datasets found in &Path2..);
                                                              %put %str(    --> Execution terminating.);

            %goto exit;
        %end;

        /*---------------------------------------------------------------------------------------*/
        /* Dataset-level comparison                                                              */
        /*---------------------------------------------------------------------------------------*/

          %*Check if dataset-level report is desired.;
            %if %sysfunc(prxmatch(/&DSRegex/i, &Detail)) %then %do;
                %let Path1Datasets = ;
                %let Path2Datasets = ;

                proc sql noprint;
                  %*Create macro variable list to rename metadata variables in Path1Datasets and
                    Path2Datasets.;
                    select  catx(' = ', name, cats(name, put(1, 3.))),
                            catx(' = ', name, cats(name, put(2, 3.)))
                      into :Path1Datasets separated by ' ',
                           :Path2Datasets separated by ' '
                        from dictionary.columns
                          where libname  = 'WORK'
                            and memname  = 'PATH1DATASETS'
                            and    name ne 'memname';
                quit;

                %local     NewDatasets
                       MissingDatasets;

              %*Compare dataset-level metadata.;
                data Datasets;
                    merge Path1Datasets (in = a rename = (&Path1Datasets))
                          Path2Datasets (in = b rename = (&Path2Datasets)) end = eof;
                    by memname;

                   *New and missing datasets;
                    length     NewDatasets
                           MissingDatasets $1000;
                    retain     NewDatasets
                           MissingDatasets;

                    if a and not b then     NewDatasets = catx(', ',     NewDatasets, memname);
                    if b and not a then MissingDatasets = catx(', ', MissingDatasets, memname);

                    if eof then do;
                        call symputx('NewDatasets',         NewDatasets);
                        call symputx('MissingDatasets', MissingDatasets);
                    end;

                   *Dataset column;
                    dataset = cats('<td bgcolor = "lightblue"><b><font face = "courier new">', memname, '</font></b></td>');

                   *File creation and modification datetimes;
                    format created1 modified1 created2 modified2 datetime19.;

                    filename1 = filename('Path1DS', cats("&Path1\", memname, '.sas7bdat'));
                    filename2 = filename('Path2DS', cats("&Path2\", memname, '.sas7bdat'));

                    if a then do;
                        fileopen1  = fopen('Path1DS');
                            size1     = input(finfo(fileopen1, 'File Size (bytes)'), best.);
                            created1  = input(finfo(fileopen1, 'Create Time'),       datetime19.);
                            modified1 = input(finfo(fileopen1, 'Last Modified'),     datetime19.);
                        fileclose1 = fclose(fileopen1);
                    end;

                    if b then do;
                        fileopen2  = fopen('Path2DS');
                            size2     = input(finfo(fileopen2, 'File Size (bytes)'), best.);
                            created2  = input(finfo(fileopen2, 'Create Time'),       datetime19.);
                            modified2 = input(finfo(fileopen2, 'Last Modified'),     datetime19.);
                        fileclose2 = fclose(fileopen2);
                    end;

                   *Metadata disparities traffic lighting;
                    array metadata  (*) $255 nvar  nobs  size  created  modified ;
                    array metadata1 (*)      nvar1 nobs1 size1 created1 modified1;
                    array metadata2 (*)      nvar2 nobs2 size2 created2 modified2;

                    do i = 1 to dim(metadata);
                             if metadata1(i) gt metadata2(i) then metadata(i) = cats('<td align = "center"><b><font face = "courier new" color = "green">', putn(metadata2(i), vformat(metadata2(i))), '</font></b></td>');
                        else if metadata1(i)  = metadata2(i) then metadata(i) = cats('<td align = "center"><b><font face = "courier new" color = "black">', putn(metadata2(i), vformat(metadata2(i))), '</font></b></td>');
                        else if metadata1(i) lt metadata2(i) then metadata(i) = cats('<td align = "center"><b><font face = "courier new" color = "red">',   putn(metadata2(i), vformat(metadata2(i))), '</font></b></td>');
                    end;

                    if a and b;
                run;

              %*Add indicator variables of first and last observations in dataset-level metadata
                comparison dataset.;
                data Datasets;
                    set Datasets end = eof;

                    if _n_ = 1 then firstds = 1;
                    if eof = 1 then lastds  = 1;
                run;
            %end;

        /*---------------------------------------------------------------------------------------*/
        /* Variable-level comparison                                                             */
        /*---------------------------------------------------------------------------------------*/

          %*Check if variable-level report is desired.;
            %if %sysfunc(prxmatch(/&VarRegex/i, &Detail)) %then %do;
                proc sql noprint;
                  %*&Path1 variables;
                    create table Path1Variables as
                        select *, upcase(memname) as dataset, upcase(name) as variable
                            from dictionary.columns
                              where libname = '_PATH1_'
                    order by dataset, variable;
                  %*&Path2 variables;
                    create table Path2Variables as
                        select *, upcase(memname) as dataset, upcase(name) as variable
                            from dictionary.columns
                              where libname = '_PATH2_'
                    order by dataset, variable;
                  %*Capture common datasets.;
                    select distinct  memname
                      into          :CommonDatasets separated by '" "'
                        from dictionary.tables
                          where libname = '_PATH1_'
                            and memname in (
                              select distinct memname
                                from dictionary.tables
                                  where libname = '_PATH2_');
                  %*Create macro variable list to rename metadata variables in Path1Variables and
                    Path2Variables.;
                    select  catx(' = ', name, cats(name, put(1, 3.))),
                            catx(' = ', name, cats(name, put(2, 3.)))
                      into :Path1Variables separated by ' ',
                           :Path2Variables separated by ' '
                        from dictionary.columns
                          where libname  = 'WORK'
                            and memname  = 'PATH1VARIABLES'
                            and    name ne 'dataset'
                            and    name ne 'variable';
                quit;

              %*Compare variable-level metadata.;
                data Variables;
                    merge Path1Variables (in = a rename = (&Path1Variables))
                          Path2Variables (in = b rename = (&Path2Variables));
                    where dataset in ("&CommonDatasets");
                    by    dataset variable;

                   *Character metadata which excludes length;
                    array metadata  (*) $1000 type  format  informat  label ;
                    array metadata1 (*) $     type1 format1 informat1 label1;
                    array metadata2 (*) $     type2 format2 informat2 label2;

                   *Retain variable lists for each metadata attribute within each dataset.;
                                                length variables1  variables2                                 length $1000;
                                                retain variables1  variables2  type  format  informat  label  length      ;
                    if first.dataset then call missing(variables1, variables2, type, format, informat, label, length)     ;

                   *If variable exists in both dataset;
                    if a and b then do;
                        do i = 1 to dim(metadata);
                           *If metadata does not match, concatenate variable name to variable list
                            for a given attribute.;
                            if metadata1(i) ne metadata2(i) then metadata(i) = catx(', ', metadata(i), variable);
                        end;

                       *Numeric metadata (length);
                        if length1 ne length2 then length = catx(', ', length, variable);
                    end;
                   *Concatenate variable names only in new snapshot to new variable list.;
                    else if a and not b then variables1 = catx(', ', variables1, variable);
                   *Concatenate variable names only in old snapshot to old variable list.;
                    else if b and not a then variables2 = catx(', ', variables2, variable);

                   *Count number of attributes with non-missing variable lists per dataset and
                    output the last record for each dataset.;
                    if last.dataset then do;
                        RowsPerDS = strip(put(7 - cmiss(variables1, variables2, type, format, informat, label, length), 8.));
                        output;
                    end;
                run;

              %*Add indicator variables of first and last observations in variable-level metadata
                comparison dataset.;
                data Variables;
                    set Variables end = eof;

                    if _n_ = 1 then firstvar = 1;
                    if eof = 1 then lastvar  = 1;
                run;
            %end;

        /*---------------------------------------------------------------------------------------*/
        /* ID-level comparison                                                                   */
        /*---------------------------------------------------------------------------------------*/

          %*Check if ID-level report is desired.;
            %if %sysfunc(prxmatch(/&IDRegex/i, &Detail)) %then %do;
                proc sql noprint;
                  %*Capture Path1 datasets and IDVar type (in case IDVar type differs between
                    datasets) in each dataset for datasets in both snapshots.;
                    select  distinct memname,              type
                      into :ID1Datasets separated by ' ', :ID1Types separated by ' '
                        from dictionary.columns
                          where libname   like '_PATH1_'
                            and memname not in (%sysfunc(prxchange(%str(s/, */' '/), -1,
                                                                   %nrbquote('&NewDatasets' '&MissingDatasets'))))
                            and    name      = "&IDVar";
                  %*Capture Path2 datasets and IDVar type (in case IDVar type differs between
                    datasets) in each dataset for datasets in both snapshots.;
                    select  distinct memname,              type
                      into :ID2Datasets separated by ' ', :ID2Types separated by ' '
                        from dictionary.columns
                          where libname   like '_PATH2_'
                            and memname not in (%sysfunc(prxchange(%str(s/, */' '/), -1,
                                                         %nrbquote('&NewDatasets' '&MissingDatasets'))))
                            and    name      = "&IDVar";
                quit;

              %*If both snapshots contain datasets with IDvar;
                %if %nrbquote(&ID1Datasets) ne and %nrbquote(&ID2Datasets) ne %then %do;
                    proc sql;
                      %*Create dataset with one record per Path1 dataset per IDVar value and count
                        number of records per dataset per IDVar value.;
                        create table Path1IDs as
                        /*  Iterate over datasets in Path1  */
                            %do i = 1 %to %sysfunc(countw(&ID1Datasets));
                                %let Dataset = %scan(&ID1Datasets, &i);
                                %let Type    = %scan(&ID1Types,    &i);

                                    select "&Dataset"                                       as Dataset,
                                       /*  Convert IDVar to character if numeric  */
                                           %if &Type = char %then                              &IDVar;
                                                            %else strip(put(&IDVar, best.)) as &IDVar;,
                                           count(1)                                         as nID2
                                        from _path1_.&Dataset
                                    group by &IDVar

                                %if &i lt %sysfunc(countw(&ID1Datasets)) %then outer union corr;
                                                                         %else %str(order by Dataset, &IDVar;);
                            %end;
                      %*Create dataset with one record per Path1 dataset per IDVar value and count
                        number of records per dataset per IDVar value.;
                        create table Path2IDs as
                        /*  Iterate over datasets in Path2  */
                            %do i = 1 %to %sysfunc(countw(&ID2Datasets));
                                %let Dataset = %scan(&ID2Datasets, &i);
                                %let Type    = %scan(&ID2Types,    &i);

                                    select "&Dataset"                                       as Dataset,
                                       /*  Convert IDVar to character if numeric  */
                                           %if &Type = char %then                              &IDVar;
                                                            %else strip(put(&IDVar, best.)) as &IDVar;,
                                           count(1)                                         as nID2
                                        from _path2_.&Dataset
                                    group by &IDVar

                                %if &i lt %sysfunc(countw(&ID2Datasets)) %then outer union corr;
                                                                         %else %str(order by Dataset, &IDVar;);
                            %end;
                    quit;

                  %*Compare variable-level metadata.;
                    data IDs (drop = &IDVar);
                        length &IDVar $255;
                        merge Path1IDs (in = a)
                              Path2IDs (in = b);
                        by Dataset &IDVar;

                       *Retain ID lists for each metadata attribute within each dataset.;
                                                    length Path1IDs  Path2IDs  RecordsLost $1000;
                                                    retain Path1IDs  Path2IDs  RecordsLost      ;
                        if first.Dataset then call missing(Path1IDs, Path2IDs, RecordsLost)     ;

                       *Concatenate IDVar values which only appear in new snapshot to ID list.;
                             if a and not b then Path1IDs = catx(', ', Path1IDs, &IDVar);
                       *Concatenate IDVar values which only appear in old snapshot to ID list.;
                        else if b and not a then Path2IDs = catx(', ', Path2IDs, &IDVar);

                       *Concatenate IDVar values with fewer records than previous snapshot to ID
                        list along with change in number of records.;
                        if .z lt nID1 lt nID2 then RecordsLost = catx(', ',
                                                                      RecordsLost,
                                                                      catx(' (',
                                                                           &IDVar,
                                                                           cats(put(nid1 - nid2, 8.),
                                                                                ')'
                                                                                )
                                                                           )
                                                                      );

                       *Count number of attributes with non-missing ID lists per dataset and output
                        the last record for each dataset.;
                        if last.Dataset then do;
                            RowsPerDS = strip(put(3 - cmiss(Path1IDs, Path2IDs, RecordsLost), 8.));
                            if RowsPerDS ne '0' then output;
                        end;
                    run;

                  %*Add indicator variables of first and last observations in ID-level metadata
                    comparison dataset.;
                    data IDs;
                        set IDs end = eof;

                        if _n_ = 1 then firstid = 1;
                        if eof = 1 then lastid  = 1;
                    run;

                  %*Check existence of ID-level metadata differences.;
                    proc sql noprint;
                        select  count(distinct 1)
                          into :IDNobs
                            from IDs;
                    quit;
                %end;
              %*If either snapshot does not contain any datasets with IDVar remove ID-level
                summary.;
                %else %do;
                    %if %nrbquote(&ID1Datasets) = and
                        %nrbquote(&ID2Datasets) = %then %put %str(    --> No datasets contain &IDVar.. ID-level summary will not be generated.);
                    %else %if %nrbquote(&ID1Datasets) = %then %do;
                        %put %str(    --> No datasets in &Path1);
                        %put %str(    -->     contain &IDVar.. ID-level summary will not be generated.);
                    %end;
                    %else %do;
                        %put %str(    --> No datasets in &Path2);
                        %put %str(    -->     contain &IDVar.. ID-level summary will not be generated.);
                    %end;

                    %if &Detail = ALL %then %let Detail = DATASET VARIABLE;
                                      %else %let Detail = %sysfunc(prxchange(s/all ?|id ?|subject ?//i, -1, &Detail));
                %end;

                %if &Detail = %then %do;
                    %put %str(    --> No summaries can be generated. Please review snapshotCompare arguments.);
                    %put %str(    -->     Execution terminating.);

                    %goto exit;
                %end;
            %end;

    %put;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put --> *    EMAIL                                                                                    *;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put;

      %*Generate hyperlinks to data snapshots.;
          %*NET USE prints information about network drives to the command prompt.  You can save
            the output to a text file from which the information can be read into SAS.;
            %let tempDir = %sysfunc(pathname(work));
            %sysexec(net use > &tempDir\_drives_.txt);

            data Text (drop  = Text
                   /*  Only keep records with network drive info.  */
                       where = (prxmatch('/^[a-z]:/i', Drive)));
               *Read in the text file.;
                infile "&tempDir\_drives_.txt" pad dsd dlm = '~';
                input   Text $char255. @@;

               *Parse out drive letter.;
                Drive = scan(Text, 1);
               *Parse out drive name (DFS = distributed file system, a Windows network drive thing).;
                DFS   = scan(Text, 2);

               *Verify that the path you want to hyperlink is a local or network drive.;
                if upcase("&PATH1") =: upcase(strip(Drive))
                    then Hyperlink1 = cats('<a href = "file:///', tranwrd("&Path1", substr("&Path1", 1, 2), strip(DFS)), '"', ">current</a>");
                    else Hyperlink1 = cats('<a href = "file:///',         "&Path1"                                     , '"', ">current</a>");
                if upcase("&PATH2") =: upcase(strip(Drive))
                    then Hyperlink2 = cats('<a href = "file:///', tranwrd("&Path2", substr("&Path2", 1, 2), strip(DFS)), '"', ">previous</a>");
                    else Hyperlink2 = cats('<a href = "file:///',         "&Path2"                                     , '"', ">previous</a>");

                if upcase("&PATH1") =: upcase(strip(Drive)) then call symputx('Hyperlink1', Hyperlink1);
                if upcase("&PATH2") =: upcase(strip(Drive)) then call symputx('Hyperlink2', Hyperlink2);
            run;

          %*Delete the text file.;
            x "del &tempDir\_drives_.txt";

      %*Select sender email address.;
        options emailid   = "%sysfunc(propcase(
                               %sysfunc(prxchange(%str(s/[._]/ /), -1,
                                 %scan(
                                   %scan(&emails, 1, %str( ))
                                 , 1, @)
                               ))
                             )) <%scan(&emails, 1, %str( ))>";

      %*Generate email file.;
        filename snapshot
                   /*  Iterate over each email address.  */
                 email(%do i = 1 %to %sysfunc(countw(&Emails, %str( )));
                           %let Email = %scan(&Emails, &i, %str( ));

                           "%sysfunc(propcase(%sysfunc(prxchange(%str(s/[._]/ /), -1, %scan(&Email, 1, @))))) <&Email>"
                       %end;)
                 ct      = 'text/html'
                 subject = "&Project Snapshot Comparison - &Today";

          %*Write snapshot comparison report to email.;
            data _null_;
                file snapshot notitles;

               *Stack metadata comparison datasets.;
                set %if %sysfunc(prxmatch(/&DSRegex/i,  &Detail)) %then Datasets  (in = a);
                    %if %sysfunc(prxmatch(/&VarRegex/i, &Detail)) %then Variables (in = b);
                    %if %sysfunc(prxmatch(/&IDRegex/i,  &Detail)) %then IDs       (in = c); end = eof;

               *Print greeting and comparison message with hyperlinks to email.;
                if _n_ = 1 then do;
                         if input("&systime", time5.) lt 43200 then put "<p>Good morning,</p>";
                    else if input("&systime", time5.) lt 64800 then put "<p>Good afternoon,</p>";
                                                               else put "<p><yawn> Good evening,</p>";
                                                                    put "<p>Comparison between datasets in the <b>%nrbquote(&Hyperlink1)</b> and <b>%nrbquote(&Hyperlink2)</b> snapshots:</p>";
                end;

               *Dataset-level report;
                %if %sysfunc(prxmatch(/&DSRegex/i, &Detail)) %then %do;
                    if a then do;
                       *Dataset-level expressions which only print once;
                        if firstds = 1 then do;
                            put '<p><b><font size = "16">Dataset-level comparison:</font></b></p>';

                           *Print new and/or missing dataset.;
                            if cmiss("&NewDatasets", "&MissingDatasets") = 0 then do;
                                put '<p><font face = "courier new"><b>--> New datasets</b>:&nbsp;&nbsp;&nbsp;&nbsp; '"&NewDatasets<br>";
                                put                               '<b>--> Missing datasets</b>: '                    "&MissingDatasets</font></p>";
                            end;
                            else if not missing(    "&NewDatasets") then
                                put '<p><font face = "courier new"><b>--> New datasets</b>: '                        "&NewDatasets</font></p>";
                            else if not missing("&MissingDatasets") then
                                put '<p><font face = "courier new"><b>--> Missing datasets</b>: '                    "&MissingDatasets</font></p>";

                           *Define HTML table format.;
                            put '<head>';
                            put '<style>';
                            put '  table, th, td {';
                            put '    border:          1px solid black;';
                            put '    border-collapse: collapse;';
                            put '  }';
                            put '  th, td {';
                            put '    padding: 5px;';
                            put '  }';
                            put '  th {';
                            put '    background-color: lightblue;';
                            put '  }';
                            put '</head>';
                            put '</style>';

                           *Open HTML table and define dataset-level table headers.;
                            put '<table style = "width:80%">';
                            put '  <tr>';
                            put '    <th style = "width:12%" align = "left">Dataset               </th>';
                            put '    <th                                   >Number of Variables   </th>';
                            put '    <th                                   >Number of Observations</th>';
                            put '    <th                                   >File Size (bytes)     </th>';
                            put '    <th                                   >Created               </th>';
                            put '    <th                                   >Modified              </th>';
                            put '  </tr>';
                        end;

                           *Print dataset-level metadata comparisons.;
                            put '  <tr>';
                            put '  ' dataset;
                            put '  ' nvar;
                            put '  ' nobs;
                            put '  ' size;
                            put '  ' created;
                            put '  ' modified;
                            put '  </tr>';

                       *Close HTML table and print traffic lighting footnotes.;
                        if lastds = 1 then do;
                            put '</table><p>';
                            put '<font color = "green">Green text</font> indicates an increase since the previous snapshot.<br>';
                            put '<font color = "black">Black text</font> indicates no change since the previous snapshot.<br>';
                            put '<font color = "red">Red text</font> indicates a decrease since the previous snapshot.<br>';
                            put '</p>';
                        end;
                    end;
                %end;

               *Variable-level report;
                %if %sysfunc(prxmatch(/&VarRegex/i, &Detail)) %then %do;
                    if b then do;
                       *Variable-level expressions which only print once;
                        if firstvar = 1 then put '<p><b><font size = "16">Variable-level comparison:</font></b></p>';

                       *Open HTML table and define variable-level table headers.;
                        if cmiss(variables1, variables2, type, format, informat, label, length) lt 7 and varcheck1 = 0 then do;
                            put '<table style = "width:80%">';
                            put '  <tr>';
                            put '    <th style = "width:12%" align = "left" >Dataset</th>';
                            put '    <th style = "width:12%" align = "right">Attribute</th>';
                            put '    <th                     align = "left" >Variables with Mismatches</th>';
                            put '  </tr>';
                        end;

                        retain varcheck1 0;

                        array labels    (7) $50 _temporary_ ('New'       'Missing'   'Type' 'Format' 'Informat' 'Label' 'Length');
                        array variables (*) $                 variables1  variables2  type   format   informat   label   length;

                           *Print variable-level metadata comparisons.;
                            if cmiss(variables1, variables2, type, format, informat, label, length) lt 7 then do;
                                varcheck1 = 1;

                                do i = 1 to dim(variables);
                                    if not missing(variables(i)) then do;
                                             put '  <tr>';
                                        if varcheck2 ne 1 then
                                             put '  <td rowspan = "' RowsPerDS '" bgcolor = "lightblue"><b><font face = "courier new">'                    dataset'</font></b></td>';
                                        if i gt 3         then
                                             put '  <td align   = "right"        ><b><font face = "courier new">'                  labels(i)'</font></b></td>';
                                        else put '  <td align   = "right"        ><b><font face = "courier new" color = "red">'    labels(i)'</font></b></td>';
                                             put '  <td                          ><b><font face = "courier new">'               variables(i)'</font></b></td>';
                                             put '  </tr>';

                                        varcheck2 = 1;
                                    end;
                                end;
                            end;

                       *Close HTML table.;
                        if lastvar = 1 then do;
                            if varcheck1 = 0 then put '<b><font style = "courier new">&nbsp;&nbsp;&nbsp;&nbsp;No discrepancies found.</font></b><br>';
                                             else put '</table><p>';
                        end;
                    end;
                %end;

               *ID-level report;
                %if %sysfunc(prxmatch(/&IDRegex/i, &Detail)) %then %do;
                    if c then do;
                       *ID-level expressions which only print once;
                        if firstid = 1 then do;
                            put '<p><b><font size = "16">ID-level comparison:</font></b></p>';

                           *Open HTML table and define ID-level table headers.;
                            put '<table style = "width:80%">';
                            put '  <tr>';
                            put '    <th style = "width:12%" align = "left" >   Dataset      </th>';
                            put '    <th style = "width:12%" align = "right">' "&IDVar Status</th>";
                            put '    <th                     align = "left" >' "&IDVar.s     </th>";
                            put '  </tr>';
                        end;

                           *Print variable-level metadata comparisons.;
                            if not missing(Path1IDs) then do;
                                    put '  <tr>';
                                if idcheck ne 1 then
                                    put '  <td rowspan = "' RowsPerDS '" bgcolor = "lightblue"><b><font face = "courier new">'          dataset'</font></b></td>';
                                    put '  <td align   = "right"        ><b><font face = "courier new" color = "green">New</font></b></td>';
                                    put '  <td align   = "left"         ><b><font face = "courier new">'         Path1IDs'</font></b></td>';
                                    put '  </tr>';

                                idcheck = 1;
                            end;

                            if not missing(Path2IDs) then do;
                                    put '  <tr>';
                                if idcheck ne 1 then
                                    put '  <td rowspan = "' RowsPerDS '" bgcolor = "lightblue"><b><font face = "courier new">'            dataset'</font></b></td>';
                                    put '  <td align   = "right"        ><b><font face = "courier new" color = "red">Missing</font></b></td>';
                                    put '  <td align   = "left"         ><b><font face = "courier new">'           Path2IDs'</font></b></td>';
                                    put '  </tr>';

                                idcheck = 1;
                            end;

                            if not missing(RecordsLost) then do;
                                    put '  <tr>';
                                if idcheck ne 1 then
                                    put '  <td rowspan = "' RowsPerDS '" bgcolor = "lightblue"><b><font face = "courier new">'                  dataset'</font></b></td>';
                                    put '  <td align   = "right"        ><b><font face = "courier new" color = "red">Fewer Records</font></b></td>';
                                    put '  <td align   = "left"         ><b><font face = "courier new">'              RecordsLost'</font></b></td>';
                                    put '  </tr>';

                                idcheck = 1;
                            end;

                       *Close HTML table.;
                        if lastid = 1 then put '</table><p>';
                    end;
                   *Print message if no ID-level discrepancies exist.;
                    else if &IDNobs = 0 and eof then do;
                        put '<p><b><font size = "16">ID-level comparison:</font></b></p>';
                        put '<b><font style = "courier new">&nbsp;&nbsp;&nbsp;&nbsp;No discrepancies found.</font></b><br>';
                    end;
                %end;
            run;

      %*Clear email FILENAME.;
        filename snapshot clear;

    %exit:

    %put;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put --> *    snapshotCompare TERMINATING EXECUTION                                                    *;
    %put --> *---------------------------------------------------------------------------------------------*;
    %put;

  %*Return options to their original settings.;
    options &xwait_ &notes_ &threads_ compress = &compress_ linesize = &linesize_;

%mend  snapshotCompare;
