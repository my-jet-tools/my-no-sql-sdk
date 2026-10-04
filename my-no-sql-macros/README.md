# my-no-sql-macros

Proc-macros which turn a plain struct into a MyNoSql entity.

```rust
#[my_no_sql_macros::my_no_sql_entity("test")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
    pub my_field_1: String,
    pub my_field_2: usize,
}
```

* adds the `partition_key`, `row_key` and `time_stamp` fields and implements the `MyNoSqlEntity` and `MyNoSqlEntitySerializer` traits, which makes it possible to use the struct in Reader and Writer for the table "test";
* `#[my_no_sql_macros::my_no_sql_entity(table_name:"test", with_expires:true)]` also adds the `expires: Timestamp` field; a default (unset) `expires` is left out of the JSON, the way a default `time_stamp` is;
* the generated code refers to `my_no_sql_sdk::…` paths, so the crate which uses the macro has to depend on `my-no-sql-sdk` as well (with its feature `macros` the same macro is available as `my_no_sql_sdk::macros::my_no_sql_entity`);
* `enum_of_my_no_sql_entity` (on an enum) and `enum_model` (on the struct of each case) keep several entity shapes in one table — see [enum_test.rs](../my-no-sql-tests/src/macros_tests/enum_test.rs). A case with a row key is one row, a case without one is a whole partition; a row which fits both is read as the case with the row key, whatever the order of declaration. Keep the two in different partitions: `get_enum_case_models_by_partition_key` (reader and writer) turns every row of the partition into the partition's case and panics on a row of another case.
