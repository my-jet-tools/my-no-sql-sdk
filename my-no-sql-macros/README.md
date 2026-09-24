
```rust
#[my_no_sql_macros::my_no_sql_entity("test")]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TestEntity {
    pub my_field_1: String,
    pub my_field_2: usize,
}
```

* implements all the fields and traits make it possible to use in Reader and Writer for the table "test";
