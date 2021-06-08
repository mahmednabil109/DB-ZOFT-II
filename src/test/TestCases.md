# check list
- [X] add index data to `metadata.csv` file
- [X] handle overflowPages with IndexPages
- [X] handle deletion of the IndexPage file in the disk
- [X] test old testcases for milestone 1.
- [X] refactor code to handle update and search
- [X] decide for the null values and the index.
- [ ] handling the null values by adding new column and new row.
- [ ] adding the validation to the index.
    - checking for the tableName & columnNames if they exists or not
    - checking for the type of the input dose it match the type of the column ot not.
    - handle the queries on out of range values.
- [X] handle the empty query.
- [ ] finish the Unit test of the dbApp as a whole.

# test case
- [X] test the updating of the `metadata.csv` file.
- [X] test consistancy between index creation after and before data fill of the table.
- [ ] test correctness of the `AND` `OR` `XOR` operators with actual queries.
    - test and with and without index.
    - 
- [ ] test correctness of the result of empty, single , multiple and range queries with help of python script.
- [ ] test correctness and the consistancy of the queries after and before deleting related and non-related rows.
