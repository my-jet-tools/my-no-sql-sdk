use types_reader::TokensObject;

use crate::{entity_utils::*, EnumModelParameters};

pub fn generate(
    attr: proc_macro2::TokenStream,
    input: proc_macro2::TokenStream,
) -> Result<proc_macro::TokenStream, syn::Error> {
    let parameters: TokensObject = attr.try_into()?;

    let parameters = EnumModelParameters::try_from(&parameters)?;

    let partition_key = parameters.partition_key;
    let row_key = parameters.row_key;

    let (struct_name, new_struct) =
        compile_src_with_new_fields(&input, false, row_key.is_none(), true);

    let fn_get_time_stamp = get_fn_get_time_stamp_token();

    let fn_serialize_deserialize = get_fn_standard_serialize_deserialize();

    let impl_additional_traits =
        impl_single_entity_or_entities_trait(&struct_name, partition_key, row_key);

    let (row_key, fn_get_row_key_body) = if let Some(row_key) = row_key {
        let row_key = quote::quote!(pub const ROW_KEY: Option<&'static str> = Some(#row_key););

        let fn_get_row_key_body = quote::quote!(Self::ROW_KEY.unwrap());

        (row_key, fn_get_row_key_body)
    } else {
        let row_key = quote::quote!(
            pub const ROW_KEY: Option<&'static str> = None;
        );
        let fn_get_row_key_body = quote::quote!(&self.row_key);

        (row_key, fn_get_row_key_body)
    };

    let result = quote::quote! {
        #new_struct

        impl #struct_name{
            pub const PARTITION_KEY:&'static str = #partition_key;
            #row_key
        }


        impl my_no_sql_sdk::abstractions::MyNoSqlEntity for #struct_name {

        const TABLE_NAME: &'static str = "";
        const LAZY_DESERIALIZATION: bool = true;


        fn get_partition_key(&self) -> &str {
            Self::PARTITION_KEY
        }

        fn get_row_key(&self) -> &str {
            #fn_get_row_key_body
        }

        #fn_get_time_stamp


       }

         impl my_no_sql_sdk::abstractions::MyNoSqlEntitySerializer for #struct_name {
           #fn_serialize_deserialize
         }

       #impl_additional_traits

    };

    Ok(result.into())
}

fn impl_single_entity_or_entities_trait(
    struct_name: &syn::Ident,
    partition_key: &str,
    row_key: Option<&str>,
) -> proc_macro2::TokenStream {
    match row_key {
        Some(row_key) => quote::quote! {
            impl my_no_sql_sdk::abstractions::GetMyNoSqlEntity for #struct_name {
                const PARTITION_KEY: &'static str = #partition_key;
                const ROW_KEY: &'static str = #row_key;
            }
        },
        None => quote::quote! {
            impl my_no_sql_sdk::abstractions::GetMyNoSqlEntitiesByPartitionKey for #struct_name {
                const PARTITION_KEY: &'static str = #partition_key;
            }
        },
    }
}
