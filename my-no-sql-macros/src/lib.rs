use proc_macro::TokenStream;
use types_reader::macros::*;

extern crate proc_macro;
mod entity_utils;
mod enum_model;
mod enum_of_my_no_sql_entity;
mod my_no_sql_entity;

#[derive(MacrosParameters)]
struct MyNoSqlEntityParameters<'s> {
    #[default]
    pub table_name: &'s str,
    pub with_expires: Option<bool>,
}

#[proc_macro_attribute]
pub fn my_no_sql_entity(attr: TokenStream, input: TokenStream) -> TokenStream {
    match crate::my_no_sql_entity::generate(attr.into(), input) {
        Ok(result) => result.into(),
        Err(err) => err.into_compile_error().into(),
    }
}

#[derive(MacrosParameters)]
struct EnumOfMyNoSqlEntityParameters<'s> {
    #[default]
    pub table_name: &'s str,

    // For each case fn unwrap_case_xx(&self)->&Model will be generated
    #[has_attribute]
    pub generate_unwraps: bool,
}

#[proc_macro_attribute]
pub fn enum_of_my_no_sql_entity(attr: TokenStream, input: TokenStream) -> TokenStream {
    match crate::enum_of_my_no_sql_entity::generate(attr.into(), input.into()) {
        Ok(result) => result.into(),
        Err(err) => err.into_compile_error().into(),
    }
}

#[derive(MacrosParameters)]
struct EnumModelParameters<'s> {
    pub partition_key: &'s str,
    pub row_key: Option<&'s str>,
}
#[proc_macro_attribute]
pub fn enum_model(attr: TokenStream, input: TokenStream) -> TokenStream {
    match crate::enum_model::generate(attr.into(), input.into()) {
        Ok(result) => result.into(),
        Err(err) => err.into_compile_error().into(),
    }
}

#[proc_macro]
pub fn time_stamp_init(_input: TokenStream) -> TokenStream {
    quote::quote! {
        "".to_string()
    }
    .into()
}
