CREATE TABLE testks.jph_test (
    last_name text,
    first_name text,
    json_blob text,
    PRIMARY KEY (last_name, first_name)
) WITH CLUSTERING ORDER BY (first_name ASC)
;
