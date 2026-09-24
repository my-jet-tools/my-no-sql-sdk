use syn::Ident;
use types_reader::StructProperty;

use crate::entity_utils::*;

pub fn generate_base_impl(
    struct_name: &Ident,
    derive: proc_macro2::TokenStream,
    fields: &[StructProperty],
    table_name: &str,
    render_expires: bool,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let new_struct = compile_struct_with_new_fields(struct_name, derive, fields, render_expires)?;

    let fn_get_time_stamp = get_fn_get_time_stamp_token();

    let fn_serialize_deserialize = get_fn_standard_serialize_deserialize();

    let result = quote::quote! {

        #new_struct

        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

            const TABLE_NAME: &'static str = #table_name;

            const LAZY_DESERIALIZATION: bool = false;

            fn get_partition_key(&self) -> &str {
                &self.partition_key
            }

            fn get_row_key(&self) -> &str {
                &self.row_key
            }

            #fn_get_time_stamp


        }

        impl my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer for #struct_name {
            #fn_serialize_deserialize
        }

    };

    Ok(result)
}
