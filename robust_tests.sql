CREATE TABLE ROBUST_TEST    (ID INT, 
COL1 VARCHAR, COL2 VARCHAR     , COL3   VARCHAR, 
COL4 VARCHAR);

INSERT INTO ROBUST_TEST VALUES (1, 'Normal', 'Data', 'Here', 'Now');
INSERT INTO ROBUST_TEST VALUES ("hello", 'Normal', 'Data', 'Here', 'Now');

INSERT INTO ROBUST_TEST VALUES (2, 'Too', 'Many', 'Columns', 'Here', 'Extra1', 'Extra2', 'Extra3', 'Extra4', 'Extra5', 'Extra6', 'Extra7');

INSERT INTO ROBUST_TEST VALUES (3, 'ThisStringIsWayTooLongAndWillExceedTheEightyByteLimitWeSetEarlierWhichShouldTriggerTheCheck', 'A', 'B', 'C');

INSERT INTO ROBUST_TEST VALUES (4, '', '', '', '');

INSERT      INTO     ROBUST_TEST       VALUES    (    5    ,    'Spaced'   ,     'Out'    ,  ''  ,  ''  )    ;

INSERT INTO ROBUST_TEST VALUES (
6, 
'New', 
'Line', 
'Test', 
''
);

INSERT INTO ROBUST_TEST VALUES (7, 'No', 'Semi', 'Colon', 'Here')

INSERT INTO ROBUST_TEST VALUES (8,'A','B','C','D'),(9,'E','F','G','H');

INSERT INTO ROBUST_TEST VALUES (-10, 'Negative', 'Key', '', '');

INSERT INTO ROBUST_TEST VALUES (99.99, 'Float', 'Key', '', '');
INSERT INTO ROBUST_TEST VALUES (99, 'Float', 'Key', '', '');

INSERT INTO ROBUST_TEST VALUES (999999999999, 'Massive', 'Key', '', '');

SELECT * FROM ROBUST_TEST WHERE ID = -10;

SELECT * FROM ROBUST_TEST WHERE ID = 404;

SELECT * FROM ROBUST_TEST;